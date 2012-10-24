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

static int is_modis_apid ( int apid ) {
    return apid >= MODIS_APID_MIN && apid <= MODIS_APID_MAX;
}

static void debug_output ( char const * packet ) {

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

static void print_time ( char const * format, unsigned char * time ) {
    char str[UTC_TIME_SIZE];
    int i = 1;
#ifdef HAVE_SDPTOOLKIT
    if( PGS_TD_EOSAMtoUTC( time, str ) == PGS_S_SUCCESS ) {
        i = 0;
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
    return ( fwrite( output->data, output->size, 1, output->file ) == 1 );
}

int main ( int argc, char ** argv ) 
{
    int retval = 0;
    /* args */
    int opt, file_count;
    FILE * cnst_file = NULL;
    char * cnst_file_name = NULL;
    /* statistics */
    int files_processed = 0, file_pkts_written, total_pkts_written = 0;
    unsigned char first_time[TIME_SIZE], last_time[TIME_SIZE];
    double first_time_tai, last_time_tai;
    /* global static structures */
    input_t * input;
    input_t ** ord_input;
    output_t output;
    /* local temp vars */
    int i, j, cur_input_idx = -1, last_cnt = 0, packet_cnt, apid;
    input_t * cur_input;
    int needs_processing, cmpres;
    char * packet_time, * packet_pos;
    size_t file_pos;

    memset( last_time, 0, sizeof( last_time ) );
    output.size = 0;
    output.file = stdout;

    while( ( opt = getopt( argc, argv, "o:c:") ) != -1 ) {
        switch( opt ) {
        case 'o':
            if( output.file != stdout ) {
                fprintf( stderr, "invalid duplicate option -- 'o'\n" );
                return 1;
            }
            output.file = fopen( optarg, "wb" );
            if( output.file == NULL ) {
                fprintf( stderr, "Can't open output file %s\n", optarg );
                return 1;
            }
            break;
        case 'c':
            if( cnst_file != NULL ) {
                fprintf( stderr, "invalid duplicate option -- 'c'\n" );
                return 1;
            }
            cnst_file_name = optarg;
            cnst_file = fopen( optarg, "wb" );
            if( cnst_file == NULL ) {
                fprintf( stderr, "Can't open constructor file %s\n", optarg );
                return 1;
            }
            break;
        default:
            fprintf( stderr, 
                "Usage: %s [-o output] [-c constructor] <input>*\n", argv[0] );
            return 1;
        }
    }

    file_count = argc - optind;
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

    /* initialize input files */
    for( i = 0; i < file_count; ++i ) {
        /* open file */
        input[i].name = argv[i+optind];
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
        do {
            apid = get_packet_apid( input[i].packet );
        } while( !is_modis_apid( apid ) && next_packet( &input[i] ) );
        if( !is_modis_apid( apid ) ) {
            fprintf( stderr, "File %s contains no modis packets, skipping\n",
                input[i].name );
            fclose( input[i].file );
            input[i].file = NULL;
            continue;
        }

        fprintf( stderr, "Reading %s\n", input[i].name );
        ord_input[i] = &input[i];
    }

    /* sort inputs by start time */
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

    if( ord_input[0] == NULL ) {
        fprintf( stderr, "No valid input files provided\n" );
        retval = 1;
        goto cleanup;
    }

    /* initialize start & stoptime */
    cur_input_idx = 0;
    memcpy( first_time, ord_input[0]->data + TIME_OFFSET, TIME_SIZE );
    memcpy( last_time, ord_input[0]->data + TIME_OFFSET, TIME_SIZE );

    /* process files in order of starttime */
    while( ( cur_input = ord_input[cur_input_idx++] ) != NULL ) {
        fprintf( stderr, "Processing %s\n", cur_input->name );

        file_pkts_written = 0;
        /* determine if file has interesting packets */
        needs_processing = 1;
        if( files_processed != 0 ) {
            /* skip all packets before last_time */
            cmpres = -1;
            do {
                apid = get_packet_apid( cur_input->packet );
                if( is_modis_apid( apid ) ) {
                    packet_time = cur_input->packet + TIME_OFFSET;
                    cmpres = memcmp( packet_time, last_time, TIME_SIZE );
                }
            } while( ( !is_modis_apid( apid ) || cmpres < 0 ) 
                    && next_packet( cur_input ) );
            
            if( !is_modis_apid( apid ) || cmpres < 0 ) {
                needs_processing = 0;
                fprintf( stderr, "File is fully overlapped\n" );
            } else {
                /*remember position*/
                packet_pos = cur_input->packet;
                file_pos = ftell( cur_input->file );
                if( file_pos < 0 ) {
                    fprintf( stderr, "Can't get file position\n" );
                    return 1;
                }
                file_pos -= cur_input->size;
                /*search for packet cnt = last+1*/
                packet_cnt = get_packet_count( cur_input->packet );
                while( cmpres == 0 && 
                        packet_cnt != ( ( last_cnt + 1 ) & 0x3fff ) &&
                        next_packet( cur_input ) ) {
                    if( is_modis_apid( get_packet_apid( input->packet ) ) ) {
                        packet_time = cur_input->packet + TIME_OFFSET;    
                        cmpres = memcmp( packet_time, last_time, TIME_SIZE );       
                        packet_cnt = get_packet_count( cur_input->packet );
                    }
                }
                if( cmpres == 0 && 
                        packet_cnt != ( ( last_cnt + 1 ) & 0x3fff ) ) {
                    needs_processing = 0;
                    fprintf( stderr, "File is fully overlapped, v2\n" );
                } else if( cmpres > 0 ) {
                    /*if not found, return to remembered position & report gap*/
                    print_time( "Warning: gap between files from %s", 
                        last_time );
                    print_time( " to %s\n", ( unsigned char * )packet_time );

                    if( fseek( cur_input->file, file_pos, SEEK_SET) ) 
                    {
                        fprintf( stderr, "Can't set file position" );
                        return 1;
                    }
                    cur_input->packet = packet_pos;
                    cur_input->size = fread( cur_input->data, 1, FILE_READ_SIZE, 
                        cur_input->file );
                    if( cur_input->data + cur_input->size 
                            < cur_input->packet + NIGHT_PACKET_SIZE ) {
                        fprintf( stderr, "Can't read enough data from file\n" );
                        return 1;
                    }
                    cur_input->packet_len = 
                        get_packet_length( cur_input->packet );
                    if( cur_input->data + cur_input->size 
                            < cur_input->packet + cur_input->packet_len ) {
                        fprintf( stderr, "Can't read enough data from file\n" );
                        return 1;   
                    }
                }
            }
        }

        if( needs_processing ) {
            fprintf( stderr, "Writing packets from %s\n", cur_input->name );
            do {
                apid = get_packet_apid( cur_input->packet );
                if( !is_modis_apid( apid ) ) {
                    continue;
                }   

                packet_cnt = get_packet_count( cur_input->packet );
                packet_time = cur_input->packet + TIME_OFFSET;

                if( packet_cnt != ( ( last_cnt + 1 ) & 0x3fff ) &&
                        memcmp( packet_time, last_time, TIME_SIZE ) == 0 ) {
                    if( ord_input[cur_input_idx] != NULL && 
                            memcmp( ord_input[cur_input_idx]->packet + 
                            TIME_OFFSET, packet_time, TIME_SIZE ) < 0 ) {
                        fprintf( stderr, 
                            "Gap inside file, trying to fix with next one\n");
                        break;
                    } else {
                        print_time( "Gap inside file from %s", last_time );
                        print_time( " to %s\n", ( unsigned char *)packet_time );
                    }
                } 

                if( !write_packet( cur_input, &output ) ) {
                    fprintf( stderr, "Can't write to output\n" );
                    retval = 1;
                    goto cleanup;
                }
                file_pkts_written++;
                total_pkts_written++;
                memcpy( last_time, packet_time, TIME_SIZE );
                last_cnt = packet_cnt;                 
            } while( next_packet( cur_input ) );
            files_processed++;
        }

        fclose( cur_input->file );
        cur_input->file = NULL;
        fprintf( stderr, "Finished %s, %d packets written\n", 
            cur_input->name, file_pkts_written );
    } 

    flush_buffer( &output );

#ifdef HAVE_SDPTOOLKIT
    print_time( "starttime=%s\n", first_time );
    print_time( "stoptime =%s\n", last_time );
    if( PGS_TD_EOSAMtoTAI( first_time, &first_time_tai ) == PGS_S_SUCCESS &&
            PGS_TD_EOSAMtoTAI( last_time, &last_time_tai ) == PGS_S_SUCCESS ) {
        fprintf( stderr, "granule length =%f\n", 
            last_time_tai - first_time_tai );
    }
#endif

    if( cnst_file ) {
        fprintf( stderr, "Writing constructor record to %s\n", cnst_file_name );

        memset( output.data, 0 , CNSTR_SIZE );
        memset( &output.data[0x33], 1, 1 );
  
        memcpy( &output.data[0x50], first_time, 8 ); 
        memcpy( &output.data[0x58], last_time, 8 ); 

        memcpy( &output.data[0x16c], first_time, 8 ); 
        memcpy( &output.data[0x174], last_time, 8 ); 

        output.data[0x74] = ( total_pkts_written >> 24 ) & 0xff;
        output.data[0x75] = ( total_pkts_written >> 16 ) & 0xff;
        output.data[0x76] = ( total_pkts_written >> 8  ) & 0xff;
        output.data[0x77] =   total_pkts_written         & 0xff;

        memset( &output.data[0x93], 1, 1 );
        memset( &output.data[0xa3], 1, 1 );
        memset( &output.data[0xf7], 2, 1 );

        memset( &output.data[0x167], 1, 1 );

        if( fwrite( output.data, CNSTR_SIZE, 1, cnst_file ) != 1 ) {
            fprintf( stderr, "Can't write constructor record\n" );
            retval = 1;
        }
    }

cleanup: 
    if( output.file != stdout ) {
        fclose( output.file );
        output.file = NULL;
    }

    if( cnst_file ) {
        fclose( cnst_file );
        cnst_file = NULL;
    }

    free( input );
    input = NULL;

    free( ord_input );
    ord_input = NULL;

    return retval;
}
