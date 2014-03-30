
#define _FILE_OFFSET_BITS 64
#include <cstdio>

#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <unistd.h>
#include <cstdlib>
#include <mpi.h>


#define MPI_CHECK( block )                                              \
  do {                                                                  \
    int retval;                                                         \
    if( (retval = block) != 0 ) {                                       \
      char error_string[MPI_MAX_ERROR_STRING];                          \
      int length;                                                       \
      MPI_Error_string( retval, error_string, &length);                 \
      std::cerr << __FILE__ << ": " << __LINE__                         \
                << ": MPI call failed: "                                \
                << error_string                                         \
                << std::endl;                                           \
      exit(1);                                                          \
    }                                                                   \
  } while(0)


size_t total_size = 1L << 36;
//size_t total_size = 1L << 32;
std::string filename_prefix( "/pic/projects/grappa/iotests/" );
int repeat = 3;

int mpi_mycore;
int mpi_cores;

//size_t mpi_count_blocks( size_t block_size, size_t 

void mpi_write_shared(std::string filename, char * buffer, size_t size) {
    MPI_Status status;
    MPI_File outfile;
    MPI_Datatype datatype;
    MPI_Info info;

    // Stupid MPI uses a signed integer for the count of data elements
    // to write. On all the machines we use, this means the max count
    // is 2^31-1. To work around this, we create a datatype large
    // enough that we never need a count value larger than 2^30.

    // move this many gigabyte chunks
    const size_t gigabyte = 1L << 30;
    size_t datatype_count = size / gigabyte;

    // if there will be any bytes left over, move those too
    if( size & (gigabyte - 1) ) datatype_count++;

    MPI_CHECK( MPI_Info_create( &info ) );
//     // disable collective io on lustre for <= 1GB
//     MPI_CHECK( MPI_Info_set( info, "romio_lustre_ds_in_coll", "1073741825" ) );

    // it's important to set lustre striping properly for performance.
    // we're supposed to be able to do this through MPI ROMIO, but mpi installations are not always configured to allow this.
    // if not, then before saving a file, run a command like this to create it:
    //    lfs setstripe -c -1 mpi_write_shared.bin -s 32m
    // below are what we'd like to do.
//     // we want 32MB lustre blocks, but this probably doesn't do anything
//     MPI_CHECK( MPI_Info_set( info, "striping_unit", "33554432" ) ); 
//     // we want to use all lustre OSTs, but this probably doesn't do anything
//     MPI_CHECK( MPI_Info_set( info, "striping_factor", "-1" ) ); 

    // use same size for collective buffering
    MPI_CHECK( MPI_Info_set( info, "cb_buffer_size", "33554432" ) );
//     // disable collective buffering for writing
//     MPI_CHECK( MPI_Info_set( info, "romio_cb_write", "disable" ) );
//     // disable collective buffering for writing
//     MPI_CHECK( MPI_Info_set( info, "romio_cb_read", "disable" ) );
    // disable data sieving for writing
    MPI_CHECK( MPI_Info_set( info, "romio_ds_write", "disable" ) );
    // disable data sieving for writing
    MPI_CHECK( MPI_Info_set( info, "romio_ds_read", "disable" ) );
    // disable independent file operations
    MPI_CHECK( MPI_Info_set( info, "romio_no_indep_rw", "true" ) );

    MPI_CHECK( MPI_Info_set( info, "direct_read", "true" ) );
    MPI_CHECK( MPI_Info_set( info, "direct_write", "true" ) );
    //    MPI_CHECK( MPI_Info_set( info, "romio_lustre_co_ratio", "1" ) );
    
    int mode = MPI_MODE_CREATE | MPI_MODE_WRONLY;
    MPI_CHECK( MPI_File_open( MPI_COMM_WORLD, filename.c_str(), mode, info, &outfile ) );

    if( mpi_mycore == 0 ) {
      MPI_CHECK( MPI_File_get_info( outfile, &info ) );
      int nkeys;
      MPI_Info_get_nkeys(info, &nkeys);
      printf("MPI File Info: nkeys = %d\n",nkeys);
      for( int i=0; i<nkeys; i++ ) {
        char key[MPI_MAX_INFO_KEY], value[MPI_MAX_INFO_VAL];
        int  valuelen, flag;
        
        MPI_Info_get_nthkey(info, i, key);
        MPI_Info_get_valuelen(info, key, &valuelen, &flag);
        MPI_Info_get(info, key, valuelen+1, value, &flag);
        printf("MPI File Info: [%2d] key = %25s, value = %s\n",i,key,value);
      }
      fflush(stdout);
    }


    MPI_CHECK( MPI_File_set_size( outfile, 0 ) );

    //MPI_CHECK( MPI_File_write_shared( outfile, buffer, size, MPI_BYTE, &status ) );
    //MPI_CHECK( MPI_File_write_shared( outfile, buffer, 1, datatype, &status ) );
    //MPI_CHECK( MPI_File_write_shared( outfile, buffer_remaining, datatype_remaining, MPI_BYTE, &status ) );

    MPI_CHECK( MPI_Allreduce( MPI_IN_PLACE, &datatype_count, 1, MPI_INT64_T, MPI_MAX, MPI_COMM_WORLD ) );

    for( int i = 0; i < datatype_count; ++i ) {
      if( size > gigabyte ) {
        MPI_CHECK( MPI_File_write_shared( outfile, buffer, gigabyte, MPI_BYTE, &status ) );
        size -= gigabyte;
        buffer += gigabyte;
      } else {
        MPI_CHECK( MPI_File_write_shared( outfile, buffer, size, MPI_BYTE, &status ) );
      }
    }
    
    MPI_CHECK( MPI_File_close( &outfile ) );
}

void mpi_read_shared(std::string filename, char * buffer, size_t size) {
    MPI_Status status;
    MPI_File infile;
    MPI_Datatype datatype;
    MPI_Info info;

    // Stupid MPI uses a signed integer for the count of data elements
    // to write. On all the machines we use, this means the max count
    // is 2^31-1. To work around this, we create a datatype large
    // enough that we never need a count value larger than 2^30.

    // move this many gigabyte chunks
    const size_t gigabyte = 1L << 30;
    size_t datatype_count = size / gigabyte;

    // if there will be any bytes left over, move those too
    if( size & (gigabyte - 1) ) datatype_count++;

    MPI_CHECK( MPI_Info_create( &info ) );
//     // disable collective io on lustre for <= 1GB
//     MPI_CHECK( MPI_Info_set( info, "romio_lustre_ds_in_coll", "1073741825" ) );
//     // we want 32MB lustre blocks, but this probably doesn't do anything
//     MPI_CHECK( MPI_Info_set( info, "striping_unit", "33554432" ) ); 
//     // we want to use all lustre OSTs, but this probably doesn't do anything
//     MPI_CHECK( MPI_Info_set( info, "striping_factor", "-1" ) ); 

    // use same size for collective buffering
    MPI_CHECK( MPI_Info_set( info, "cb_buffer_size", "33554432" ) );
//     // disable collective buffering for writing
//     MPI_CHECK( MPI_Info_set( info, "romio_cb_write", "disable" ) );
//     // disable collective buffering for writing
//     MPI_CHECK( MPI_Info_set( info, "romio_cb_read", "disable" ) );
    // disable data sieving for writing
    MPI_CHECK( MPI_Info_set( info, "romio_ds_write", "disable" ) );
    // disable data sieving for writing
    MPI_CHECK( MPI_Info_set( info, "romio_ds_read", "disable" ) );
    // disable independent file operations
    MPI_CHECK( MPI_Info_set( info, "romio_no_indep_rw", "true" ) );

    MPI_CHECK( MPI_Info_set( info, "direct_read", "true" ) );
    MPI_CHECK( MPI_Info_set( info, "direct_write", "true" ) );
    //    MPI_CHECK( MPI_Info_set( info, "romio_lustre_co_ratio", "1" ) );
    
    int mode = MPI_MODE_RDONLY;
    MPI_CHECK( MPI_File_open( MPI_COMM_WORLD, filename.c_str(), mode, info, &infile ) );

    if( mpi_mycore == 0 ) {
      MPI_CHECK( MPI_File_get_info( infile, &info ) );
      int nkeys;
      MPI_Info_get_nkeys(info, &nkeys);
      printf("MPI File Info: nkeys = %d\n",nkeys);
      for( int i=0; i<nkeys; i++ ) {
        char key[MPI_MAX_INFO_KEY], value[MPI_MAX_INFO_VAL];
        int  valuelen, flag;
        
        MPI_Info_get_nthkey(info, i, key);
        MPI_Info_get_valuelen(info, key, &valuelen, &flag);
        MPI_Info_get(info, key, valuelen+1, value, &flag);
        printf("MPI File Info: [%2d] key = %25s, value = %s\n",i,key,value);
      }
      fflush(stdout);
    }


    //MPI_CHECK( MPI_File_write_shared( infile, buffer, size, MPI_BYTE, &status ) );
    //MPI_CHECK( MPI_File_write_shared( infile, buffer, 1, datatype, &status ) );
    //MPI_CHECK( MPI_File_write_shared( infile, buffer_remaining, datatype_remaining, MPI_BYTE, &status ) );

    // compute number of rounds required to read entire file
    MPI_CHECK( MPI_Allreduce( MPI_IN_PLACE, &datatype_count, 1, MPI_INT64_T, MPI_MAX, MPI_COMM_WORLD ) );

    for( int i = 0; i < datatype_count; ++i ) {
      if( size > gigabyte ) {
        MPI_CHECK( MPI_File_read_shared( infile, buffer, gigabyte, MPI_BYTE, &status ) );
        size -= gigabyte;
        buffer += gigabyte;
      } else {
        MPI_CHECK( MPI_File_read_shared( infile, buffer, size, MPI_BYTE, &status ) );
      }
    }
    
    MPI_CHECK( MPI_File_close( &infile ) );
}

// void mpi_read_shared(std::string filename, char * buffer, size_t size) {
//     MPI_Status status;
//     MPI_File outfile;
//     MPI_Datatype datatype;

//     // Stupid MPI uses a signed integer for the count of data elements
//     // to write. On all the machines we use, this means the max count
//     // is 2^31-1. To work around this, we create a datatype large
//     // enough that we never need a count value larger than 2^30.
//     const size_t gigabyte = 1L << 30;
//     size_t datatype_count = size / gigabyte;
//     if( size & (gigabyte - 1) ) datatype_count++;

//     int mode = MPI_MODE_CREATE | MPI_MODE_WRONLY;
//     MPI_CHECK( MPI_File_open( MPI_COMM_WORLD, filename.c_str(), mode, MPI_INFO_NULL, &outfile ) );

//     MPI_CHECK( MPI_File_set_size( outfile, 0 ) );

//     //MPI_CHECK( MPI_File_write_shared( outfile, buffer, size, MPI_BYTE, &status ) );
//     //MPI_CHECK( MPI_File_write_shared( outfile, buffer, 1, datatype, &status ) );
//     //MPI_CHECK( MPI_File_write_shared( outfile, buffer_remaining, datatype_remaining, MPI_BYTE, &status ) );

//     MPI_CHECK( MPI_Allreduce( MPI_IN_PLACE, &datatype_count, 1, MPI_INT64_T, MPI_MAX, MPI_COMM_WORLD ) );

//     for( int i = 0; i < datatype_count; ++i ) {
//       if( size > gigabyte ) {
//         MPI_CHECK( MPI_File_write_shared( outfile, buffer, gigabyte, MPI_BYTE, &status ) );
//         size -= gigabyte;
//         buffer += gigabyte;
//       } else {
//         MPI_CHECK( MPI_File_write_shared( outfile, buffer, size, MPI_BYTE, &status ) );
//       }
//     }
    
//     MPI_CHECK( MPI_File_close( &outfile ) );
// }

// void mpi_write_all(std::string filename, void * buffer, size_t size) {
//     MPI_Status status;
//     MPI_File outfile;
//     MPI_Datatype datatype;

//     int mode = MPI_MODE_CREATE | MPI_MODE_WRONLY;
//     MPI_CHECK( MPI_File_open( MPI_COMM_WORLD, filename.c_str(), mode, MPI_INFO_NULL, &outfile ) );

//     MPI_CHECK( MPI_File_set_size( outfile, 0 ) );

//     MPI_CHECK( MPI_File_write_shared( outfile, buffer, size, MPI_BYTE, &status ) );
    
//     MPI_CHECK( MPI_File_close( &outfile ) );
// }

void mpi_write_at_all(std::string filename, void * buffer, size_t size) {
    MPI_Status status;
    MPI_File outfile;
    MPI_Datatype datatype;

    int mode = MPI_MODE_CREATE | MPI_MODE_WRONLY;
    MPI_CHECK( MPI_File_open( MPI_COMM_WORLD, filename.c_str(), mode, MPI_INFO_NULL, &outfile ) );

    MPI_CHECK( MPI_File_set_size( outfile, 0 ) );

    size_t offset = size;
    MPI_CHECK( MPI_Scan( MPI_IN_PLACE, &offset, 1, MPI_INT64_T, MPI_SUM, MPI_COMM_WORLD ) );

    MPI_CHECK( MPI_File_write_at_all( outfile, offset, buffer, size, MPI_BYTE, &status ) );
    
    MPI_CHECK( MPI_File_close( &outfile ) );
}


void posix_write(std::string filename, void * buffer, size_t size) {
  FILE* f;
  std::stringstream ss;
  ss << filename << "." << mpi_mycore << ".bin";
  //std::cout << "Writing to " << ss.str().c_str() << std::endl;

  size_t offset = size;
  MPI_CHECK( MPI_Scan( MPI_IN_PLACE, &offset, 1, MPI_INT64_T, MPI_SUM, MPI_COMM_WORLD ) );

  f = fopen( ss.str().c_str(), "wb" );
  fwrite( buffer, size, sizeof(char), f );
  fflush( f );
  fclose( f );
  fsync( fileno( f ) );
}

void posix_read_shared(std::string filename, void * buffer, size_t size) {
  FILE* f;
  int64_t retval;

  size_t offset = size;
  MPI_CHECK( MPI_Scan( MPI_IN_PLACE, &offset, 1, MPI_INT64_T, MPI_SUM, MPI_COMM_WORLD ) );

  f = fopen( filename.c_str(), "r" );
  if( NULL == f ) { std::cerr << "Couldn't open " << filename << std::endl; exit(1); }
  
  retval = fseeko( f, offset, SEEK_SET );
  if( retval != 0 ) { std::cerr << "Couldn't seek to " << offset << " in " << filename << std::endl; exit(1); }

  retval = fread( buffer, sizeof(char), size, f );
  if( retval != size ) { 
    std::cerr << "Problem reading " << filename
              << " (got " << retval << " bytes, expected " << size
              << ")" << std::endl; exit(1);
  }

  retval = fclose( f );
  if( retval != 0 ) { std::cerr << "Couldn't close " << filename << std::endl; exit(1); }
}


int main( int argc, char * argv[] ) {
  MPI_CHECK( MPI_Init( &argc, &argv ) );
  MPI_CHECK( MPI_Comm_rank( MPI_COMM_WORLD, &mpi_mycore ) );
  MPI_CHECK( MPI_Comm_size( MPI_COMM_WORLD, &mpi_cores ) );
  
  if( mpi_mycore == 0 ) std::cout << "Running." << std::endl;
  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

  size_t size_per_core = total_size / mpi_cores;
  char * buf = NULL;
//   std::cout << "Total size: " << total_size
//             << " Size per core: " << size_per_core
//             << std::endl;
  MPI_Alloc_mem( size_per_core * sizeof(char), MPI_INFO_NULL, &buf );

  for( int i = 0; i < repeat; ++i ) {
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

    double start = MPI_Wtime();
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
    //mpi_write_shared( filename_prefix + "mpi_write_shared.bin", buf, size_per_core );
    //posix_write( filename_prefix + "posix_write", buf, size_per_core );
    //mpi_write_at_all( filename_prefix + "mpi_write_at_all.bin", buf, size_per_core );

    mpi_read_shared( filename_prefix + "mpi_write_shared.bin", buf, size_per_core );
    //posix_read_shared( filename_prefix + "mpi_write_shared.bin", buf, size_per_core );
      //posix_read_shared( filename_prefix + "posix_read-36.bin", buf, size_per_core );
    sync();
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
    double runtime = MPI_Wtime() - start;
    double bandwidth = total_size / runtime / 1000000;
    if( mpi_mycore == 0 ) {
      std::cout << "MPI collective write " << total_size << " bytes in " 
                << runtime << " seconds: " 
                << bandwidth << " MB/s." << std::endl;
    }
  }
  
  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  MPI_Free_mem( buf );

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  if( mpi_mycore == 0 ) std::cout << "Done." << std::endl;

  return 0;
}

