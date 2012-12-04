#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

#ifdef HAVE_SDPTOOLKIT
#include <PGS_TD.h>
#endif

#define MAX_FILES 100

#define FILE_PREFETCH_SIZE 32768
#define FILE_READ_SIZE ( 8 * FILE_PREFETCH_SIZE )
#define FILE_WRITE_SIZE FILE_READ_SIZE
#define BUFFER_SIZE ( 2 * FILE_READ_SIZE )

#define NIGHT_PACKET_SIZE 276
#define DAY_PACKET_SIZE 642
#define TIME_SIZE 8
#define PRIM_HDR_SIZE 6
#define CNSTR_SIZE 384
#define UTC_TIME_SIZE 27

#define MODIS_APID_MIN 64
#define MODIS_APID_MAX 127

#define PACKET_LEN_OFFSET 4
#define PACKET_CNT_OFFSET 2
#define SEC_HDR_OFFSET PRIM_HDR_SIZE
#define TIME_OFFSET SEC_HDR_OFFSET

extern char *optarg;
extern int optind, opterr, optopt;

typedef struct {
    char * name;
    FILE * file;
    size_t size;
    char * packet;
    size_t packet_len;
    char data[BUFFER_SIZE];
} input_t;

typedef struct {
    char * name;
    FILE * file;
    size_t size;
    char data[BUFFER_SIZE];
} output_t;


static unsigned int get_int16 ( char const * packet, int offset )
{
    return ( ( ( ( unsigned char ) packet[offset] ) << 8 ) 
        + ( ( unsigned char ) packet[offset+1] ) );
}

static unsigned int get_int32 ( char const * packet, int offset )
{
    return ( get_int16( packet, offset ) << 16 ) 
        + get_int16( packet, offset + 2 );
}

static size_t get_packet_length ( char const * packet )
{
    return get_int16( packet, PACKET_LEN_OFFSET ) + PRIM_HDR_SIZE + 1;
}

static int get_packet_count ( char const * packet )
{
    return get_int16( packet, PACKET_CNT_OFFSET ) & 0x3fff;
}

static int get_packet_apid ( char const * packet )
{
    return get_int16( packet, 0 ) & 0x7ff;
}

static int is_modis_packet( input_t const * input ) 
{
    int apid;
    size_t length;

    apid = get_packet_apid( input->packet );
    if( apid >= MODIS_APID_MIN && apid <= MODIS_APID_MAX ) {
        length = get_packet_length( input->packet );
        return length == NIGHT_PACKET_SIZE || length == DAY_PACKET_SIZE;
    } else {
        return 0;
    }
}

static void debug_output ( char const * packet ) 
{
    int type = ( ( ( unsigned char ) packet[0] ) >> 4 ) & 0x1;
    int apid = get_packet_apid( packet );
    int sflg = get_int16( packet, 2 ) >> 14;
    int cnt = get_packet_count( packet );
    int days = get_int16( packet, 6 );
    unsigned int ms = get_int32( packet, 8 );
    int mcs = get_int16( packet, 12 );
    fprintf( stderr, "%d %d %d %d %d %u %d\n", 
        type, apid, sflg, cnt, days, ms, mcs );
}

static void print_time ( char const * format, unsigned char * time ) 
{
    char str[UTC_TIME_SIZE];
    int i = 1;
#ifdef HAVE_SDPTOOLKIT
    if( time[0] & 128 ) {
        if( PGS_TD_EOSPMtoUTC( time, str ) == PGS_S_SUCCESS ) {
            i = 0;
        }
    } else {
        if( PGS_TD_EOSAMtoUTC( time, str ) == PGS_S_SUCCESS ) {
            i = 0;
        }
    }
#endif
    if( i ) {
        for( i = 0; i < TIME_SIZE; ++i ) {
            sprintf( str + 2*i, "%02x", time[i] );
        }
        str[TIME_SIZE*2] = '\0';
    }
    fprintf( stderr, format, str );
}

#ifdef HAVE_SDPTOOLKIT
static int eostime_to_tai ( unsigned char * time, double * tai ) 
{
    if( time[0] & 128 ) {
        return PGS_TD_EOSPMtoTAI( time, tai ) != PGS_S_SUCCESS;
    } else {
        return PGS_TD_EOSAMtoTAI( time, tai ) != PGS_S_SUCCESS;
    }
}
#endif

static int ensure_data ( input_t * input )
{
    if( input->packet + input->packet_len <= input->data + input->size )
        return 1;

    if( !feof( input->file ) ) {
        input->size -= ( input->packet - input->data );
        memmove( input->data, input->packet, input->size );
        input->packet = input->data;
        input->size += fread( 
            input->data + input->size, 1, FILE_READ_SIZE, input->file );
    } 

    if( input->data + input->size < input->packet + input->packet_len ) {
        if( feof( input->file ) ) {
            if( input->data + input->size < input->packet )
                fprintf( stderr, "Incomplete packet in the end of file\n" );
        } else {
            fprintf( stderr, "Can't read %d bytes for packet\n", 
                ( int ) input->packet_len );
        }
        return 0;
    } else {
        return 1;
    }
}

static int next_packet ( input_t * input )
{
    input->packet += input->packet_len;
    input->packet_len = PRIM_HDR_SIZE;
    if( !ensure_data( input ) ) {
        return 0;
    }
    input->packet_len = get_packet_length( input->packet );
    if( input->packet_len > FILE_READ_SIZE ) {
        fprintf( stderr, "Wrong packet size %d in %s\n", 
            ( int ) input->packet_len, input->name );
        debug_output( input->packet );
        return 0;
    }
    if( !ensure_data( input ) ) {
        return 0;
    }
    return 1;
}

static int write_packet ( input_t const * input, output_t * output ) 
{
    size_t rest;
    if( output->size + input->packet_len >= FILE_WRITE_SIZE ) {
        rest = FILE_WRITE_SIZE - output->size;
        memcpy( output->data + output->size, input->packet, rest );
        if( fwrite( output->data, FILE_WRITE_SIZE, 1, output->file ) != 1 ) {
            return 0;
        }
        output->size = input->packet_len - rest;
        memcpy( output->data, input->packet + rest, output->size );
    } else {
        memcpy( output->data + output->size, input->packet, input->packet_len );
        output->size += input->packet_len;
    }
    return 1;
}

static int flush_buffer ( output_t * output )
{
    if( output->file != NULL ) {
        return ( fwrite( output->data, output->size, 1, output->file ) == 1 );
    } else {
        return 1;
    }
}

static int parse_options( 
    int argc, 
    char ** argv, 
    output_t * output, 
    output_t * cnstr,
    output_t * apid957 )
{
    int opt;

    output->name = NULL;
    output->size = 0;
    output->file = stdout;

    cnstr->name = NULL;
    cnstr->size = 0;
    cnstr->file = NULL;

    while( ( opt = getopt( argc, argv, "o:c:a:") ) != -1 ) {
        switch( opt ) {
        case 'o':
            if( output->file != stdout ) {
                fprintf( stderr, "invalid duplicate option -- 'o'\n" );
                return -1;
            }
            output->name = optarg;
            output->file = fopen( optarg, "wb" );
            if( output->file == NULL ) {
                fprintf( stderr, "Can't open output file %s\n", optarg );
                return -1;
            }
            break;
        case 'c':
            if( cnstr->file != NULL ) {
                fprintf( stderr, "invalid duplicate option -- 'c'\n" );
                return -1;
            }
            cnstr->name = optarg;
            cnstr->file = fopen( optarg, "wb" );
            if( cnstr->file == NULL ) {
                fprintf( stderr, "Can't open constructor file %s\n", optarg );
                return -1;
            }
            break;
        case 'a':
            if ( apid957->file != NULL ) {
                fprintf( stderr, "invalid duplicate option -- 'a'\n" );
                return -1;    
            }
            apid957->name = optarg;
            apid957->file = fopen( optarg, "wb" );
            if( apid957->file == NULL ) {
                fprintf( stderr, "Can't open apid957 file %s\n", optarg );
                return -1;
            }
            break;
        default:
            fprintf( stderr, 
                "Usage: %s [-o output] [-c constructor] <input>*\n", argv[0] );
            return -1;
        }
    }
    return optind;
}

static void init_inputs( 
    int file_count, 
    char ** names, 
    input_t * input )
{
    int i;

    for( i = 0; i < file_count; ++i ) {
        /* open file */
        input[i].name = names[i];
        input[i].file = fopen( input[i].name, "rb" );
        if( input[i].file == NULL ) {
            fprintf( stderr, "Can't open %s, skipping\n", input[i].name );
            continue;
        }
        /* read first chunk */
        input[i].size = fread( 
            input[i].data, 1, FILE_PREFETCH_SIZE, input[i].file );
        if( input[i].size < NIGHT_PACKET_SIZE ) {
            fprintf( stderr, "File %s is too small, skipping\n", input[i].name);
            fclose( input[i].file );
            input[i].file = NULL;
            continue;
        }
        /* search for the first MODIS packet */
        input[i].packet = input[i].data;
        input[i].packet_len = get_packet_length( input[i].packet );
        while( !is_modis_packet( &input[i] ) && next_packet( &input[i] ) );
        if( !is_modis_packet( &input[i] ) ) {
            fprintf( stderr, "File %s contains no modis packets, skipping\n",
                input[i].name );
            fclose( input[i].file );
            input[i].file = NULL;
            continue;
        }

        fprintf( stderr, "Reading %s\n", input[i].name );
    }
}

static void sort_inputs(
    int file_count,
    input_t * input,
    input_t ** ord_input ) 
{
    int i, j;

    for( i = 0; i < file_count; ++i ) {
        ord_input[i] = &input[i];
    }

    for( i = 1; i < file_count; ++i ) {
        if( ord_input[i] != NULL ) {
            ord_input[file_count] = ord_input[i];
            j = i;
            while( j > 0 && ( ord_input[j-1] == NULL || 
                    memcmp( ord_input[file_count]->packet + TIME_OFFSET,
                        ord_input[j-1]->packet + TIME_OFFSET, 
                        TIME_SIZE ) < 0 ) ) {
                ord_input[j] = ord_input[j-1];
                j--;
            }
            ord_input[j] = ord_input[file_count];
        }
    }
    ord_input[file_count] = NULL;
}

static int preprocess_file(
    input_t * input,
    unsigned char last_time[TIME_SIZE],
    int last_cnt )
{
    int cmpres = -1, packet_cnt;
    char *packet_time, *packet_pos;
    long file_pos;
    
    /* skip all packets before last_time */
    do {
        if( is_modis_packet( input ) ) {
            packet_time = input->packet + TIME_OFFSET;
            cmpres = memcmp( packet_time, last_time, TIME_SIZE );
        }
    } while( ( !is_modis_packet( input ) || cmpres < 0 ) 
        && next_packet( input ) );
    
    if( !is_modis_packet( input ) || cmpres < 0 ) {
        fprintf( stderr, "File is fully overlapped\n" );
        return 0;
    } 
        
    /*remember position*/
    packet_pos = input->packet;
    file_pos = ftell( input->file );
    if( file_pos < 0 ) {
        fprintf( stderr, "Can't get file position\n" );
        return -1;
    }
    file_pos -= input->size;

    /*search for packet cnt = last+1*/
    packet_cnt = get_packet_count( input->packet );
    while( cmpres == 0 && 
            packet_cnt != ( ( last_cnt + 1 ) & 0x3fff ) &&
            next_packet( input ) ) {
        if( is_modis_packet( input ) ) {
            packet_time = input->packet + TIME_OFFSET;    
            cmpres = memcmp( packet_time, last_time, TIME_SIZE );       
            packet_cnt = get_packet_count( input->packet );
        }
    }
    
    if( cmpres == 0 && packet_cnt != ( ( last_cnt + 1 ) & 0x3fff ) ) {
        fprintf( stderr, "File is fully overlapped, v2\n" );
        return 0;
    } 
    
    /*if not found, return to remembered position & report gap*/
    if( cmpres > 0 ) {
        if( fseek( input->file, file_pos, SEEK_SET) ) 
        {
            fprintf( stderr, "Can't set file position" );
            return -1;
        }
        input->packet = packet_pos;
        input->size = fread( input->data, 1, FILE_READ_SIZE, input->file );
        if( input->data + input->size < input->packet + NIGHT_PACKET_SIZE ) {
            fprintf( stderr, "Can't read enough data from file\n" );
            return -1;
        }
        input->packet_len = get_packet_length( input->packet );
        if( input->data + input->size < input->packet + input->packet_len ) {
            fprintf( stderr, "Can't read enough data from file\n" );
            return -1;   
        }

        print_time( "Warning: gap between files from %s", last_time );
        print_time( " to %s\n", 
            ( unsigned char * )input->packet + TIME_OFFSET );
    }
    
    return 1;
}

static int process_file( 
    input_t * input, 
    output_t * output,
    output_t * apid957,
    unsigned char last_time[TIME_SIZE],
    int * last_cnt,
    char next_file_time[TIME_SIZE] ) 
{
    int packets_written = 0, packet_cnt;
    char * packet_time;

    fprintf( stderr, "Writing packets from %s\n", input->name );
    do {
        if( is_modis_packet( input ) ) {
            packet_cnt = get_packet_count( input->packet );
            packet_time = input->packet + TIME_OFFSET;

            if( packet_cnt != ( ( *last_cnt + 1 ) & 0x3fff ) &&
                    memcmp( packet_time, last_time, TIME_SIZE ) == 0 ) {
                if( next_file_time != NULL &&
                        memcmp( next_file_time, packet_time, TIME_SIZE ) < 0 ) {
                    fprintf( stderr, 
                        "Gap inside file, trying to fix with next one\n");
                    break;
                } else if( memcmp( packet_time, last_time, TIME_SIZE ) != 0 ) {
                    print_time( "Gap inside file from %s", last_time );
                    print_time( " to %s\n", ( unsigned char *)packet_time );
                }
            } 

            if( !write_packet( input, output ) ) {
                fprintf( stderr, "Can't write to output\n" );
                return -1;
            }
            packets_written++;
            memcpy( last_time, packet_time, TIME_SIZE );
            *last_cnt = packet_cnt;                     
        } else if ( get_packet_apid( input->packet ) == 957 && apid957->file ) {
            if( !write_packet( input, apid957 ) ) {
                fprintf( stderr, "Can't write to apid957\n" );
                return -1;
            }
        }
    } while( next_packet( input ) );
    fprintf( stderr, "Finished %s, %d packets written\n", 
        input->name, packets_written );
    return packets_written;
}

static int write_cnst(
    output_t * cnst,
    unsigned char const first_time[TIME_SIZE],
    unsigned char const last_time[TIME_SIZE],
    int total_pkts_written )
{
    if( cnst->file ) {
        fprintf( stderr, "Writing constructor record to %s\n", cnst->name );

        memset( cnst->data, 0 , CNSTR_SIZE );
        memset( &cnst->data[0x33], 1, 1 );
  
        memcpy( &cnst->data[0x50], first_time, 8 ); 
        memcpy( &cnst->data[0x58], last_time, 8 ); 

        memcpy( &cnst->data[0x16c], first_time, 8 ); 
        memcpy( &cnst->data[0x174], last_time, 8 ); 

        cnst->data[0x74] = ( total_pkts_written >> 24 ) & 0xff;
        cnst->data[0x75] = ( total_pkts_written >> 16 ) & 0xff;
        cnst->data[0x76] = ( total_pkts_written >> 8  ) & 0xff;
        cnst->data[0x77] =   total_pkts_written         & 0xff;

        memset( &cnst->data[0x93], 1, 1 );
        memset( &cnst->data[0xa3], 1, 1 );
        memset( &cnst->data[0xf7], 2, 1 );

        memset( &cnst->data[0x167], 1, 1 );

        if( fwrite( cnst->data, CNSTR_SIZE, 1, cnst->file ) != 1 ) {
            fprintf( stderr, "Can't write constructor record\n" );
            return -1;
        }
    }
    return 0;
}

int main ( int argc, char ** argv ) 
{
    int retval = 0;
    /* args */
    int file_count, arg_input_idx;
    /* statistics */
    int files_processed = 0, total_pkts_written = 0;
    unsigned char first_time[TIME_SIZE], last_time[TIME_SIZE];
    double first_time_tai, last_time_tai;
    /* global static structures */
    input_t * input;
    input_t ** ord_input;
    output_t output, cnst, apid957;
    /* local temp vars */
    int i, cur_input_idx, last_cnt, needs_processing, file_pkts_written;
    input_t * cur_input;
    char * next_file_time;

    arg_input_idx = parse_options( argc, argv, &output, &cnst, &apid957 );
    if( arg_input_idx == -1 ) {
        return 1;
    }
    file_count = argc - arg_input_idx;
    if( file_count > MAX_FILES ) {
        fprintf( stderr, "Too many input files\n" );
        return 1;
    }

    /* allocate buffers */
    input = ( input_t * ) malloc( sizeof( input_t ) * file_count );
    ord_input = ( input_t ** ) malloc( sizeof( input_t* ) * ( file_count+1 ) );
    for( i = 0; i <= file_count; ++i ) {
        ord_input[i] = NULL;
    }

    init_inputs( file_count, argv + arg_input_idx, input );
    sort_inputs( file_count, input, ord_input );

    if( ord_input[0] == NULL ) {
        fprintf( stderr, "No valid input files provided\n" );
        retval = 1;
        goto cleanup;
    }

    /* initialize start & stoptime */
    last_cnt = 0;
    cur_input_idx = 0;
    memcpy( first_time, ord_input[0]->packet + TIME_OFFSET, TIME_SIZE );
    memcpy( last_time, ord_input[0]->packet + TIME_OFFSET, TIME_SIZE );

    /* process files in order of starttime */
    while( ( cur_input = ord_input[cur_input_idx++] ) != NULL ) {
        fprintf( stderr, "Processing %s\n", cur_input->name );

        needs_processing = 1;
        if( files_processed != 0 ) {
            /* determine if file has interesting packets */
            needs_processing = preprocess_file( 
                cur_input, last_time, last_cnt );
            if( needs_processing == -1 ) {
                retval = 1;
                goto cleanup;
            }
        }

        if( needs_processing ) {
            next_file_time = NULL;
            if( ord_input[cur_input_idx] != NULL ) {
                next_file_time = ord_input[cur_input_idx]->packet + TIME_OFFSET;
            }
            file_pkts_written = process_file( cur_input, &output, &apid957, 
                last_time, &last_cnt, next_file_time );
            if( file_pkts_written == -1 ) {
                retval = 1;
                goto cleanup;
            }
            total_pkts_written += file_pkts_written;
            files_processed++;
        }

        fclose( cur_input->file );
        cur_input->file = NULL;
    } 

    flush_buffer( &output );
    flush_buffer( &apid957 );

#ifdef HAVE_SDPTOOLKIT
    print_time( "starttime=%s\n", first_time );
    print_time( "stoptime =%s\n", last_time );
    if( eostime_to_tai( first_time, &first_time_tai ) == 0 &&
            eostime_to_tai( last_time, &last_time_tai ) == 0 ) {
        fprintf( stderr, "granule length =%f\n", 
            last_time_tai - first_time_tai );
    }
#endif

    write_cnst( &cnst, first_time, last_time, total_pkts_written );

cleanup: 
    if( output.file != stdout ) {
        fclose( output.file );
        output.file = NULL;
    }

    if( cnst.file ) {
        fclose( cnst.file );
        cnst.file = NULL;
    }

    if( apid957.file ) {
        fclose( apid957.file );
        apid957.file = NULL;
    }

    free( input );
    input = NULL;

    free( ord_input );
    ord_input = NULL;

    return retval;
}
