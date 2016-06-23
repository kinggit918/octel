//
// Created by vic on 16-6-23.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "leveldb/c.h"

int main()
{
    leveldb_t               *db;
    leveldb_options_t       *options;
    leveldb_readoptions_t   *roptions;
    leveldb_writeoptions_t  *woptions;
    char *err = NULL;
    char *read;
    const char* dbfile  = "testdb.ldb";
    const char* key     = "kkk";
    const char* value   = "vvvvdfasdfdssssssssssssssssss";
    size_t read_len;
    /* leveldb version */
    fprintf(stderr,"version:%d.%d\n",leveldb_major_version(),leveldb_minor_version());
    /* open the database */
    options = leveldb_options_create();
    leveldb_options_set_create_if_missing(options,1);
    db = leveldb_open(options,dbfile,&err);
    if ( err != NULL )
    {
        fprintf(stderr,"Open testdb fail.\n");
        return ( EXIT_FAILURE );
    }
    /* reset error var */
    leveldb_free( err );
    err = NULL;
//    /* WRITE */
//    woptions = leveldb_writeoptions_create();
//    leveldb_put(db,woptions,key,strlen(key),value,strlen(value), &err);
//    if ( err != NULL )
//    {
//        fprintf(stderr,"Write fail.\n");
//        return ( EXIT_FAILURE );
//    }
//    leveldb_free(err);
//    err = NULL;
    /* READ */
    roptions = leveldb_readoptions_create();
    read = leveldb_get(db,roptions, key, strlen(key), &read_len, &err);
    if ( err != NULL )
    {
        fprintf(stderr, "Read fail.\n");
        return ( EXIT_FAILURE );
    }
    if (read_len >0){
        read[read_len] = '\0';
    }
    printf("%s:%s:%ld\n",key,read,read_len);
    leveldb_free( read );
    read = NULL;
    leveldb_free ( err );
    err = NULL;
//    /*************************************************/
//    /** delete **/
//    leveldb_delete(db,woptions, key, strlen(key),&err );
//    if ( err != NULL )
//    {
//        fprintf(stderr,"Delete fail.\n");
//        return ( EXIT_FAILURE );
//    }
    /************************************************/
    /* close db */
    leveldb_close( db );
    /***********************************************/
//    /* destroy */
//    leveldb_destroy_db( options, dbfile, &err );
//    if ( err != NULL )
//    {
//        fprintf(stderr, "Destory fail.\n");
//        return ( EXIT_FAILURE );
//    }
//    leveldb_free( err );
//    err = NULL;
    return EXIT_SUCCESS;
}
