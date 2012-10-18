#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

#ifdef HAVE_SDPTOOLKIT
#include <PGS_TD.h>
#endif

#define MAX_FILES 5

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

#define PACKET_LEN_OFFSET 4
#define PACKET_CNT_OFFSET 2
#define SEC_HDR_OFFSET PRIM_HDR_SIZE
#define TIME_OFFSET SEC_HDR_OFFSET

extern char *optarg;
extern int optind, opterr, optopt;

typedef struct {
    char * name;
    FILE * file;
    char header[FILE_PREFETCH_SIZE];
    size_t size;
} input_t;

typedef struct {
    size_t pos;
    size_t len;
} packet_t;

typedef struct {
    char data[BUFFER_SIZE];
    size_t size;
} buffer_t;

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

static void debug_output( char const * packet ) {

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

static int ensure_data ( 
    input_t * input, 
    packet_t * packet, 
    buffer_t * buffer )
{
    if( packet->pos + packet->len <= buffer->size )
        return 1;

    if( !feof( input->file ) ) {
        buffer->size -= packet->pos;
        memmove( buffer->data, buffer->data + packet->pos, buffer->size );
        packet->pos = 0;
        buffer->size += fread( 
            buffer->data + buffer->size, 1, FILE_READ_SIZE, input->file );
    } 

    if( buffer->size < packet->pos + packet->len ) {
        if( feof( input->file ) ) {
            if( buffer->size != packet->pos ) {
               fprintf( stderr, "Incomplete packet in the end of file\n" );
            }
        } else {
            fprintf( stderr, "Can't read %d bytes for packet\n", 
                ( int ) packet->len );
        }
        return 0;
    } else {
        return 1;
    }
}

static int next_packet ( input_t * input, packet_t * packet, buffer_t * buffer )
{
    packet->pos += packet->len;
    packet->len = PRIM_HDR_SIZE;
    if( !ensure_data( input, packet, buffer ) ) {
        return 0;
    }
    packet->len = get_packet_length( buffer->data + packet->pos );
    if( packet->len > FILE_READ_SIZE ) {
        fprintf( stderr, "Wrong packet size %d in %s\n", 
            ( int ) packet->len, input->name );
        debug_output( buffer->data + packet->pos );
        return 0;
    }
    if( !ensure_data( input, packet, buffer ) ) {
        return 0;
    }
    return 1;
}

static int write_packet ( 
    packet_t const * packet, 
    buffer_t const * input, 
    buffer_t * output,
    FILE * stream ) 
{
    size_t rest;
    if( output->size + packet->len >= FILE_WRITE_SIZE ) {
        rest = FILE_WRITE_SIZE - output->size;
        memcpy( output->data + output->size, input->data + packet->pos, rest );
        if( fwrite( output->data, FILE_WRITE_SIZE, 1, stream ) != 1 ) {
            return 0;
        }
        output->size = packet->len - rest;
        memcpy( output->data, input->data + packet->pos + rest, output->size );
    } else {
        memcpy( output->data + output->size, input->data + packet->pos, 
            packet->len );
        output->size += packet->len;
    }
    return 1;
}

static int flush_buffer ( buffer_t * output, FILE * stream )
{
    return ( fwrite( output->data, output->size, 1, stream ) == 1 );
}

int main ( int argc, char ** argv ) 
{
    int retval = 0;
    /* args */
    int opt, file_count;
    FILE * out_file = stdout;
    FILE * cnst_file = NULL;
    char * cnst_file_name = NULL;
    /* statistics */
    int files_processed = 0, file_pkts_written, total_pkts_written = 0;
    unsigned char first_time[TIME_SIZE], last_time[TIME_SIZE];
    double first_time_tai, last_time_tai;
    /* global static structures */
    input_t input[MAX_FILES];
    input_t * ord_input[MAX_FILES + 1];
    packet_t packet;
    buffer_t buffer, output;
    /* local temp vars */
    int i, j, cur_input = -1, last_cnt = 0, packet_cnt;
    int needs_processing, cmpres;
    char * packet_time;
    size_t packet_pos, file_pos;

    while( ( opt = getopt( argc, argv, "o:c:") ) != -1 ) {
        switch( opt ) {
        case 'o':
            if( out_file != stdout ) {
                fprintf( stderr, "invalid duplicate option -- 'o'\n" );
                return 1;
            }
            out_file = fopen( optarg, "wb" );
            if( out_file == NULL ) {
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

    memset( last_time, 0, sizeof( last_time ) );
    output.size = 0;
    for( i = 0; i < MAX_FILES; ++i ) {
        input[i].file = NULL;
        input[i].name = NULL;
        input[i].size = 0;
        ord_input[i] = NULL;
    }

    for( i = 0; i < file_count; ++i ) {
        input[i].name = argv[i+optind];
        input[i].file = fopen( input[i].name, "rb" );
        if( input[i].file == NULL ) {
            fprintf( stderr, "Can't open %s, skipping\n", input[i].name );
            continue;
        }

        input[i].size = fread( 
            input[i].header, 1, FILE_PREFETCH_SIZE, input[i].file );
        if( input[i].size < NIGHT_PACKET_SIZE ) {
            fprintf( stderr, "File %s is too small, skipping\n", input[i].name);
            fclose( input[i].file );
            input[i].file = NULL;
            continue;
        }

        packet.len = get_packet_length( input[i].header );
        if( packet.len != NIGHT_PACKET_SIZE && packet.len != DAY_PACKET_SIZE ) {
            fprintf( stderr, 
                "First packet has incorrect length (%d), skipping file %s\n", 
                ( int ) packet.len, input[i].name );
            debug_output( input[i].header );
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
            ord_input[MAX_FILES] = ord_input[i];
            j = i;
            while( j > 0 && ( ord_input[j-1] == NULL || 
                    memcmp( ord_input[MAX_FILES]->header + TIME_OFFSET,
                        ord_input[j-1]->header + TIME_OFFSET, 
                        TIME_SIZE ) < 0 ) ) {
                ord_input[j] = ord_input[j-1];
                j--;
            }
            ord_input[j] = ord_input[MAX_FILES];
        }
    }
    ord_input[MAX_FILES] = NULL;
    cur_input = 0;

    if( ord_input[cur_input] == NULL ) {
        fprintf( stderr, "No valid input files provided\n" );
        return 1;
    }

    memcpy( first_time, ord_input[cur_input]->header + TIME_OFFSET, TIME_SIZE );
    memcpy( last_time, ord_input[cur_input]->header + TIME_OFFSET, TIME_SIZE );

    do {
        fprintf( stderr, "Processing %s\n", ord_input[cur_input]->name );

        file_pkts_written = 0;
        buffer.size = ord_input[cur_input]->size;
        memcpy( buffer.data, ord_input[cur_input]->header, 
            ord_input[cur_input]->size );

        packet.pos = 0;
        packet.len = get_packet_length( ord_input[cur_input]->header );  

        if( files_processed != 0 ) {
            needs_processing = 1;    
            packet_time = buffer.data + packet.pos + TIME_OFFSET;
            cmpres = memcmp( packet_time, last_time, TIME_SIZE );
            while( cmpres < 0 && 
                    next_packet( ord_input[cur_input], &packet, &buffer ) ) {
                packet_time = buffer.data + packet.pos + TIME_OFFSET;    
                cmpres = memcmp( packet_time, last_time, TIME_SIZE );
            }
            if( cmpres < 0 ) {
                needs_processing = 0;
                fprintf( stderr, "File is fully overlapped\n" );
            } else {
                /*remember position*/
                packet_pos = packet.pos;
                file_pos = ftell( ord_input[cur_input]->file );
                if( file_pos < 0 ) {
                    fprintf( stderr, "Can't get file position\n" );
                    return 1;
                }
                file_pos -= buffer.size;
                /*search for packet cnt = last+1*/
                packet_cnt = get_packet_count( buffer.data + packet.pos );
                while( cmpres == 0 && 
                        packet_cnt != ( ( last_cnt + 1 ) & 0x3fff ) &&
                        next_packet( ord_input[cur_input], &packet, &buffer ) ){
                    packet_time = buffer.data + packet.pos + TIME_OFFSET;    
                    cmpres = memcmp( packet_time, last_time, TIME_SIZE );       
                    packet_cnt = get_packet_count( buffer.data + packet.pos );
                }
                if( cmpres == 0 && 
                        packet_cnt != ( ( last_cnt + 1 ) & 0x3fff ) ) {
                    needs_processing = 0;
                    fprintf( stderr, "File is fully overlapped\n" );
                } else if( cmpres > 0 ) {
                    /*if not found, return to remembered position & report gap*/
                    packet.pos = packet_pos;
                    fprintf( stderr, "Warning: gap between files\n" );
                    if( fseek( ord_input[cur_input]->file, file_pos, SEEK_SET) ) 
                    {
                        fprintf( stderr, "Can't set file position" );
                        return 1;
                    }
                    buffer.size = fread( buffer.data, 1, FILE_READ_SIZE, 
                        ord_input[cur_input]->file );
                    if( buffer.size < packet.pos + NIGHT_PACKET_SIZE ) {
                        fprintf( stderr, "Can't read enough data from file\n" );
                        return 1;
                    }
                    packet.len = get_packet_length( buffer.data + packet.pos );
                    if( buffer.size < packet.pos + packet.len ) {
                        fprintf( stderr, "Can't read enough data from file\n" );
                        return 1;   
                    }
                }
            }
        } else {
            needs_processing = 1;
        }

        if( needs_processing ) {
            fprintf( stderr, "Writing packets from %s\n", 
                ord_input[cur_input]->name );
            do {
                packet_cnt = get_packet_count( buffer.data + packet.pos );
                packet_time = buffer.data + packet.pos + TIME_OFFSET;
                if( packet_cnt != ( ( last_cnt + 1 ) & 0x3fff ) &&
                        memcmp( packet_time, last_time, TIME_SIZE ) == 0 &&
                        ord_input[cur_input+1] != NULL && 
                        memcmp( ord_input[cur_input+1]->header + TIME_OFFSET,
                            packet_time, TIME_SIZE ) < 0 ) {
                    fprintf( stderr, 
                        "Gap inside file, trying to fix with next one\n");
                    break;
                }

                if( !write_packet( &packet, &buffer, &output, out_file ) ) {
                    fprintf( stderr, "Can't write to output\n" );
                    return 1;
                }
                memcpy( last_time, packet_time, TIME_SIZE );
                last_cnt = packet_cnt;
                file_pkts_written++;
                total_pkts_written++;
            } while( next_packet( ord_input[cur_input], &packet, &buffer ) );
            files_processed++;
        }

        fclose( ord_input[cur_input]->file );
        ord_input[cur_input]->file = NULL;
        fprintf( stderr, "Finished %s, %d packets written\n", 
            ord_input[cur_input]->name, file_pkts_written );

        cur_input++;
    } while( ord_input[cur_input] != NULL );

    flush_buffer( &output, out_file );

    if( out_file != stdout ) {
        fclose( out_file );
        out_file = NULL;
    }

#ifdef HAVE_SDPTOOLKIT
    PGS_TD_EOSAMtoTAI( first_time, &first_time_tai );
    PGS_TD_EOSAMtoUTC( first_time, output.data );
    fprintf( stderr, "starttime=%s\n", output.data );

    PGS_TD_EOSAMtoTAI( last_time, &last_time_tai );
    PGS_TD_EOSAMtoUTC( last_time, output.data );
    fprintf( stderr, "stoptime =%s\n", output.data );

    fprintf( stderr, "granule length =%f\n", last_time_tai - first_time_tai );
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

        fclose( cnst_file );
        cnst_file = NULL;
    }

    return retval;
}
