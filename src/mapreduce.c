#include "../include/mapreduce.h"

struct Config {
    // In and out files
    MPI_File inFileHandler, outFileHandler;
    char *outFile;

    // Size of file
    MPI_Offset totalFileSize, localFileSize;

    // Read buffer
    char *words;
    char *receiveWords;

    // Ranks
    int world_rank, world_size;

};

struct Config config;

void init(char *inFileHandler, char *outFile) {
    MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);
    config.outFile = outFile;

    MPI_File_open(MPI_COMM_WORLD, inFileHandler, (MPI_MODE_RDWR | MPI_MODE_CREATE), MPI_INFO_NULL, &config.inFileHandler);
    MPI_File_get_size(config.inFileHandler, &config.totalFileSize);    
    if(config.world_rank == 0){
        config.words = (char*) malloc(config.totalFileSize * sizeof(char));        
        MPI_File_read(config.inFileHandler, config.words, config.totalFileSize, MPI_CHAR, MPI_STATUS_IGNORE);
        //for(int i = 0; i < config.totalFileSize; i++){
        //    printf("%c", config.words[i]);
        //}
    }
    config.localFileSize = ((config.totalFileSize / config.world_size) + 1);
    config.receiveWords = (char*) malloc(config.localFileSize * sizeof(char));        
    for(int i = 0; i < config.localFileSize; i++){
        config.receiveWords[i] = ' ';
    }

    MPI_Scatter(config.words, config.localFileSize * sizeof(char), MPI_CHAR, config.receiveWords, config.localFileSize * sizeof(char), MPI_CHAR, 0, MPI_COMM_WORLD);
    return;
}
void cleanup() {
    return;
}

struct Tuple {
    char* key;
    int value;
};

void mapReduce() {
    const char space[4] = " \n\t";
    char *token;

    token = strtok(config.receiveWords, space);

    if(config.world_rank == 3){
        while(token != NULL){
            printf( " %s\n", token );
            token = strtok(NULL, space);
        }
    }


    return;
}