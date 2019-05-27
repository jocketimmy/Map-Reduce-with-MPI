#include "../include/mapreduce.h"
#include <vector>
#include <iostream>
#include <unordered_map>
#include <string>

#define SEED_LENGTH 65
typedef uint64_t Hash;

const char           key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num         = (unsigned short*)key_seed;

struct Config {
    // In and out files
    MPI_File inFileHandler, outFileHandler;
    char *outFile;

    // Size of file
    MPI_Offset totalFileSize, localFileCount, chunk;

    // Read buffer
    char *sendBuffer;
    char *receiveBuffer;
    char *last_buffer;

    // Ranks
    int world_rank, world_size;
    int MB = 64000000;

    std::unordered_map<std::string,int> realMap;

    MPI_Datatype block;

};

struct Config config;

void handleInput(MPI_Offset offset);

void collective_read(char *inFileHandler, char *outFile) {
    MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);
    config.outFile = outFile;
    config.chunk = config.MB*sizeof(char);

    MPI_File_open(MPI_COMM_WORLD, inFileHandler, (MPI_MODE_RDWR | MPI_MODE_CREATE), MPI_INFO_NULL, &config.inFileHandler);
    MPI_File_get_size(config.inFileHandler, &config.totalFileSize);

    config.localFileCount = config.MB;

    config.receiveBuffer = (char*) malloc(config.localFileCount * sizeof(char));

    MPI_Offset offset = config.MB * sizeof(char) * config.world_size;
    int loopCount = (config.totalFileSize / offset) + 1;
    for(int i = 0; i < loopCount; i++) {
        MPI_Offset disp = offset * i;
        if(disp + (config.chunk * config.world_rank) < config.totalFileSize) {
            handleInput(disp + (config.chunk * config.world_rank));
        }
    }

    //MPI_File_close(&config.inFileHandler);

}

void handleInput(MPI_Offset offset) {
    //std::cout << config.world_rank << " rank here" << std::endl;
    int count = 0;
    if(offset > config.totalFileSize-config.chunk) {
        config.last_buffer = (char*) malloc(config.totalFileSize % config.chunk);
        config.receiveBuffer = config.last_buffer;
        count = (config.totalFileSize % config.chunk) / sizeof(char);
        config.localFileCount = count;
    }
    MPI_File_read_at(config.inFileHandler, offset, config.receiveBuffer, config.localFileCount, MPI_CHAR, MPI_STATUS_IGNORE);
    //std::cout << "Localfilecount: " << config.localFileCount << ", offset: " << offset << std::endl;

    /*
     if(offset > config.totalFileSize-config.chunk) {
        config.last_buffer = (char*) malloc(config.totalFileSize % config.chunk);
        config.sendBuffer = config.last_buffer;
        count = (config.totalFileSize % config.chunk) / sizeof(char);
        config.localFileCount = count / config.world_size;
    }
    if(config.world_rank == 0) {
        if(offset > config.totalFileSize-config.chunk) {
            MPI_File_read_at(config.inFileHandler, offset, config.sendBuffer, count, MPI_CHAR, MPI_STATUS_IGNORE);
        }
        else {
            MPI_File_read_at(config.inFileHandler, offset, config.sendBuffer, config.MB, MPI_CHAR, MPI_STATUS_IGNORE);
        }
    }

    MPI_Scatter(config.sendBuffer, config.localFileCount, MPI_CHAR, config.receiveBuffer, config.localFileCount, MPI_CHAR, 0, MPI_COMM_WORLD);
    */
    // Map and reduce the received words below
    //std::cout << "before map reduce" << std::endl;
    mapReduce();

    return;
}

void init(char *inFileHandler, char *outFile) {
    return;
    MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);
    config.outFile = outFile;
    config.chunk = config.MB*sizeof(char);

    MPI_File_open(MPI_COMM_WORLD, inFileHandler, (MPI_MODE_RDWR | MPI_MODE_CREATE), MPI_INFO_NULL, &config.inFileHandler);
    MPI_File_get_size(config.inFileHandler, &config.totalFileSize);

    if(config.world_rank == 0){
        config.sendBuffer = (char*) malloc(config.chunk);
    }
    config.localFileCount = ((config.MB / config.world_size) + 1);
    config.receiveBuffer = (char*) malloc(config.localFileCount * sizeof(char));

    //Fill buffer with spaces
    for(int i = 0; i < config.localFileCount; i++){
        config.receiveBuffer[i] = ' ';
    }

    int loopCount = (config.totalFileSize / config.MB) + 1;

    for(int i = 0; i < loopCount; i++) {
        MPI_Offset off = i*config.chunk;
        if(off < config.totalFileSize) {
            handleInput(off);
        }
    }



    // All-to-allv here to distribute tuples
    /*std::vector<std::vector<Tuple> > sendVec(config.world_size);
    Hash hash = getHash(temp, 10);
    int newHash = (int) (hash % config.world_size);
    sendVec[newHash][]
*/
    return;
}

void cleanup() {
    return;
}

struct Tuple {
    /*Tuple(char* k, int v) {
        key = k;
        value = v;
    }*/
    char* key;
    int value;
};


Hash getHash(const char *word, size_t length) {
    Hash hash = 0;
    for (off_t i = 0; i < length; i++) {
        Hash num_char = (Hash)word[i];
        Hash seed     = (Hash)key_seed_num[(i % SEED_LENGTH)];

        hash += num_char * seed * (i + 1);
    }

    return hash;
}


void mapReduce() {
    /*std::unordered_map<std::string,int> realMap = {
        {"hej", 1},
        {"potato", 4}

    };*/
    //std::unordered_map<std::string,int> realMap;
    std::unordered_map<std::string,int>::const_iterator it;
    //std::vector<std::vector<Tuple> > sendVec(config.world_size);

    int localFileSize = (config.localFileCount / 10) + 1;
    char temp[11];
    std::string strTmpz(temp);

    for(int i = 0; i < localFileSize; i++) {
        if(i == localFileSize-1 && config.localFileCount % 10 != 0) {
            for(int j = 0; j < 10; j++) {
                temp[j] = ' ';
            }
            memcpy(temp, config.receiveBuffer + (i * 10 * sizeof(char)), config.localFileCount % 10);
        }
        else {
            memcpy(temp, config.receiveBuffer + (i * 10 * sizeof(char)), 10);
        }
        temp[10] = 0;

        std::string strTmp(temp);
        if(i + 1 == localFileSize || i + 2 == localFileSize) {
            //std::cout << config.world_rank << " " << strTmp << std::endl;
        }

        strTmpz = strTmp;
        it = config.realMap.find(strTmp);
        if(it == config.realMap.end()) {
            //std::cout << "not found, you were looking for " << strTmp << std::endl;
            config.realMap.insert({strTmp, 1});
        } else {
            config.realMap[strTmp] += 1;
            //std::cout << it->first << " yay found it " << it->second << std::endl;
        }
    }
    //std::cout << strTmpz << std::endl;




    //Hash hash = getHash(temp, 10);
    //int newHash = (int) (hash % config.world_size);
    //sendVec[newHash][]

    return;
    const char space[4] = " \n\t";
    char *token;
    int in = 0;

    std::string one = "hej";
    std::string two = "timmy";

    std::vector<Tuple> test;
    /*
    char temp[10];
    memcpy(temp, config.receiveBuffer, 10);
    temp[10] = 0;

    test.push_back(Tuple());
    test[0].key = temp;
    test[0].value = 1;

    std::cout << "HEJSAN " << test[0].value << std::endl;

    for(int i = 0; i < 10; i++) {
        std::cout << "HEJSAN2 " << test[0].key[i] << std::endl;

    }*/

    std::vector<char*> buffer;
    token = strtok(config.receiveBuffer, space);
    if(config.world_rank == 3){
        while(token != NULL){
            printf("%d ", strlen(token) );

            printf("%s\n", token );
            //for(int i = 0; i < strlen(token); i++) {
            //    printf("token char is %c\n", token[i]);
            //}
            printf("size %d\n", buffer.size());
            buffer.push_back(token);
            it = config.realMap.find(one);
            /*if(it == realMap.end()) {
                std::cout << "not found" << std::endl;
            } else {
                std::cout << it->first << " yay found it" << std::endl;
            }*/
            token = strtok(NULL, space);

        }
    }
    std::cout << buffer.size() << std::endl;

    return;
}