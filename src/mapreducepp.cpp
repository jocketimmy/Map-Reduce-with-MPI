#include "../include/mapreduce.h"
#include <vector>
#include <iostream>
#include <unordered_map>
#include <string>
#include <stddef.h>

#define SEED_LENGTH 65
typedef uint64_t Hash;
const char key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num = (unsigned short*)key_seed;

//struct Tuple;
struct Tuple {
    char key[11] = {' '};
    char padd = 'x';
    int value;
};

//Struct Config
struct Config {
    // Ranks
    int world_rank, world_size;
    //Data
    int MB = 64*1024*1024;
    char* receiveBuffer = (char*) malloc(MB * sizeof(char));
    std::unordered_map<std::string,int> realMap;
    std::unordered_map<std::string,int>::const_iterator it;
    std::vector<std::vector<Tuple> > sendVec;
};

struct Config config;

int numDigits(int);
void collective_read(char*, char*);
void collective_write(char*);
void cleanup();
void reduce(char*, int);
void init(char*, char*);
void mapReduce(char*);
void distribute();
int getHash(const char*, size_t);

int numDigits(int number) {
    int digits = 0;
    while (number) {
        number /= 10;
        digits++;
    }
    return digits;
}

void collective_read(char* inFile) {
    MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);
    config.sendVec.resize(config.world_size);
    int MB = 64*1024*1024;
    //config.receiveBuffer = (char*) malloc(MB * sizeof(char));
    MPI_File inFileHandler;
    MPI_Offset totalFileSize, offset, chunk = MB * sizeof(char);
    MPI_Datatype view_type, chunk_type;
    MPI_File_open(MPI_COMM_WORLD, inFile, MPI_MODE_RDONLY, MPI_INFO_NULL, &inFileHandler);
    MPI_File_get_size(inFileHandler, &totalFileSize);
    MPI_Type_contiguous(MB, MPI_CHAR, &chunk_type);
    MPI_Type_create_resized(chunk_type, 0, MB*config.world_size, &view_type);
    MPI_Type_commit(&chunk_type);
    MPI_Type_commit(&view_type);
    MPI_File_set_view(inFileHandler, MB*config.world_rank, chunk_type, view_type, "native", MPI_INFO_NULL);
    std::cout << "world size  " <<  config.world_size <<std::endl;

    offset = (config.world_rank * chunk);
    int loopCount = (totalFileSize / (config.world_size*chunk)) + 1;
    std::cout << "looop count " <<  loopCount <<std::endl;

    for(int i = 0; i < loopCount; i++) {
        std::cout << "RANK and loop " << config.world_rank << " "  << i << std::endl;
        MPI_File_read_all(inFileHandler, config.receiveBuffer, 1, chunk_type, MPI_STATUS_IGNORE);
        if(offset < totalFileSize) {
            if(offset > totalFileSize - chunk && offset < totalFileSize) {
                config.receiveBuffer = (char*) realloc(config.receiveBuffer, totalFileSize - offset);
            }
            mapReduce(config.receiveBuffer);
            std::cout << "else mapreduce done" << std::endl;
        }
        offset += (config.world_size * chunk);
    }
    std::cout << "READ DONE" << std::endl;
	MPI_File_close(&inFileHandler);
    return;
}

void collective_write(char* outFile) {
    MPI_Request request;
    MPI_Offset tempOffset = 0, writeOffset = 0;
    MPI_File outFileHandler;
    std::vector<char> print;
    int totalbytes = 0;
    for(int i = 0; i < config.sendVec[config.world_rank].size(); i++) {
        for(int j = 0; j < 10; j++) {
            print.push_back(config.sendVec[config.world_rank][i].key[j]);
        }
        print.push_back(':');
        int count = config.sendVec[config.world_rank][i].value;
        int countSize = numDigits(count);
        countSize++;
        char tempCount[countSize];
        snprintf(tempCount, countSize, "%d", count);
        for(int j = 0; j < strlen(tempCount); j++) {
            print.push_back(tempCount[j]);
            totalbytes++;
        }
        print.push_back('\n');
        totalbytes += 12;
    }
    if(config.world_size > 1) {
        if(config.world_rank == 0) {
            tempOffset = totalbytes;
            MPI_Isend(&tempOffset, 1, MPI_INT, config.world_rank + 1, 0, MPI_COMM_WORLD, &request);
        }
        else {
            MPI_Recv(&writeOffset, 1, MPI_INT, config.world_rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if(config.world_rank != config.world_size-1) {
                tempOffset = writeOffset + totalbytes;
                MPI_Isend(&tempOffset, 1, MPI_INT, config.world_rank + 1, 0, MPI_COMM_WORLD, &request);
            }
        }
    }
	MPI_File_open(MPI_COMM_WORLD, outFile, (MPI_MODE_RDWR | MPI_MODE_CREATE), MPI_INFO_NULL, &outFileHandler);
    MPI_File_write_at(outFileHandler, writeOffset*sizeof(char), &print[0], totalbytes, MPI_CHAR, MPI_STATUS_IGNORE);
	MPI_File_close(&outFileHandler);
}

void cleanup() {
    config.realMap.clear();
    config.sendVec.clear();
    return;
}

int getHash(const char *word, size_t length) {
    Hash hash = 0;
    for (off_t i = 0; i < length; i++) {
        Hash num_char = (Hash)word[i];
        Hash seed     = (Hash)key_seed_num[(i % SEED_LENGTH)];

        hash += num_char * seed * (i + 1);
    }
    int ret = (int) (hash % config.world_size);
    return ret;
}

char* newToken = (char*) malloc(11*sizeof(char));
char* lastToken = (char*) malloc(11*sizeof(char));

void mapReduce(char* receiveBuffer) {
    const char space[33] = "+-#~()[]-=%><?&•!·_,.'/:;\" \n\t";
    //const char space[33] = "•!·_,.'/:;\" \n\t";

    char* token;
    token = strtok(receiveBuffer, space);
    while(token != NULL){
        int looop = 0;
        while(strlen(token)-looop > 10) {
            //char* newToken = (char*) malloc(11*sizeof(char));
            for(int i = 0+looop; i < 10+looop; i++) {
                newToken[i-looop] = token[i];
            }
            newToken[10] = 0;
            reduce(newToken, 1);
            looop += 10;
        }
        if(strlen(token)-looop > 0) {
            //char* lastToken = (char*) malloc(11*sizeof(char));
            for(int i = 0; i < strlen(token)-looop; i++) {
                lastToken[i] = token[i+looop];
            }
            for(int i = strlen(token)-looop; i < 10; i++) {
                lastToken[i] = ' ';
            }
            lastToken[10] = 0;
            reduce(lastToken, 1);
        }
        token = strtok(NULL, space);
    }
    return;
}

void reduce(char* temp, int val) {
    //std::cout << "REDUCE" << std::endl;

    std::string strTmp(temp);
    config.it = config.realMap.find(strTmp);
    if(config.it == config.realMap.end()) {
        int newHash = getHash(temp, 10);
        config.sendVec[newHash].emplace_back(Tuple());
        for(int k = 0; k < 11; k++) {
            config.sendVec[newHash][config.sendVec[newHash].size()-1].key[k] = temp[k];
        }
        config.sendVec[newHash][config.sendVec[newHash].size()-1].value = val;
        config.realMap.insert({strTmp, config.sendVec[newHash].size()-1});
    } else {
        int index = config.realMap[strTmp];
        int newHash = getHash(temp, 10);
        config.sendVec[newHash][index].value += val;
    }
}

void distribute() {
    MPI_Datatype struct_type;
    MPI_Request request[config.world_size];
	MPI_Status status[config.world_size];
    int counts[config.world_size] = {0};
    int flags[config.world_size] = {0};
    int blocksCount = 3;
    int blocksLength[3] = {11, 1, 1};
    MPI_Datatype types[3] = {MPI_CHAR, MPI_CHAR, MPI_INT};
    MPI_Aint offsets[3];
    offsets[0] = offsetof(Tuple, key);
    offsets[1] = offsetof(Tuple, padd);
    offsets[2] = offsetof(Tuple, value);
    MPI_Type_create_struct(blocksCount, blocksLength, offsets, types, &struct_type);
    MPI_Type_commit(&struct_type);

    for(int i = 0; i < config.sendVec.size(); i++) {
        if(i == config.world_rank) continue;
        MPI_Isend(&config.sendVec[i][0], config.sendVec[i].size(), struct_type, i, i, MPI_COMM_WORLD, &request[config.world_rank]);
    }
    int handled[config.sendVec.size()] = {0};
    int loop = config.world_size-1;
    while(loop) {
        for(int i = 0; i < config.sendVec.size(); i++) {
            if(i == config.world_rank) continue;
            if(!handled[i]) {
                MPI_Iprobe(i, config.world_rank, MPI_COMM_WORLD, &flags[i], &status[i]);
            }
            if(!handled[i] && flags[i]) {
                handled[i] = 1;
                loop--;
                MPI_Get_count(&status[i], struct_type, &counts[i]);
                if(counts[i] != 0) {
                    std::vector<Tuple> temp(counts[i]);
                    MPI_Recv(&temp[0], counts[i], struct_type, i, config.world_rank, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    for(int j = 0; j < temp.size(); j++) {
                        reduce(temp[j].key, temp[j].value);
                    }
                }
            }
        }
    }
    return;
}