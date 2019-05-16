#include "../include/mapreduce.h"

struct Config {
    // In and out files
    MPI_File inFileHandler, outFileHandler;
    char *outFile;

    // Size of file
    MPI_Offset fileSize;

    // Read buffer
    char *words;

    // Ranks
    int world_rank;

};

struct Config config;

void init(char *inFileHandler, char *outFile) {
    MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
    config.outFile = outFile;

    MPI_File_open(MPI_COMM_WORLD, inFileHandler, (MPI_MODE_RDWR | MPI_MODE_CREATE), MPI_INFO_NULL, &config.inFileHandler);

    if(config.world_rank == 0){
        MPI_File_get_size(config.inFileHandler, &config.fileSize);
        config.words = (char*) malloc(config.fileSize * sizeof(char));        
        MPI_File_read(config.inFileHandler, config.words, config.fileSize, MPI_CHAR, MPI_STATUS_IGNORE);
        printf("%c\n", config.words[0]);
    }
    return;
}
void cleanup() {
    return;
}
void mapReduce() {
    return;
}