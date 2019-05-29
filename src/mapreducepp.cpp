#include "../include/mapreduce.h"
#include <vector>
#include <iostream>
#include <unordered_map>
#include <string>

#define SEED_LENGTH 65
typedef uint64_t Hash;
const char           key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num         = (unsigned short*)key_seed;

//struct Tuple;
struct Tuple {
    char key[11];
    char padd = 'x';
    int value;
};

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
    std::unordered_map<std::string,int>::const_iterator it;
    std::vector<std::vector<Tuple> > sendVec;

    MPI_Datatype struct_type;
};

struct Config config;

void handleInput(MPI_Offset);
void reduce(char*, int);
void collective_read(char*, char*);
void init(char*, char*);
void cleanup();
void mapReduce();
void distribute();
void collective_write();
int getHash(const char*, size_t);

void collective_read(char *inFileHandler, char *outFile) {
    MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);
    config.sendVec.resize(config.world_size);

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

void collective_write() {

}

void handleInput(MPI_Offset offset) {
    int count = 0;
    if(offset > config.totalFileSize-config.chunk) {
        config.last_buffer = (char*) malloc(config.totalFileSize % config.chunk);
        config.receiveBuffer = config.last_buffer;
        count = (config.totalFileSize % config.chunk) / sizeof(char);
        config.localFileCount = count;
    }
    MPI_File_read_at(config.inFileHandler, offset, config.receiveBuffer, config.localFileCount, MPI_CHAR, MPI_STATUS_IGNORE);
    mapReduce();
    return;
}

/*void init(char *inFileHandler, char *outFile) {
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
    return;
}*/

void cleanup() {
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


void mapReduce() {
    int localFileSize = (config.localFileCount / 10) + 1;
    for(int i = 0; i < localFileSize; i++) {
        char* temp = (char*) malloc(11*sizeof(char));
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
        reduce(temp, 1);
    }
    return;
}

void reduce(char* temp, int val) {
    std::string strTmp(temp);
    config.it = config.realMap.find(strTmp);
    if(config.it == config.realMap.end()) {
        int newHash = getHash(temp, 10);
        Tuple tempTuple;
        for(int k = 0; k < 11; k++) {
            tempTuple.key[k] = temp[k];
        }
        tempTuple.value = val;
        config.sendVec[newHash].push_back(tempTuple);
        //config.sendVec[newHash][config.sendVec[newHash].size()-1].key = temp;
        //config.sendVec[newHash][config.sendVec[newHash].size()-1].value = 1;
        config.realMap.insert({strTmp, config.sendVec[newHash].size()-1});
        std::cout << config.sendVec[newHash][config.sendVec[newHash].size()-1].value << " NO you did not found it, you wanted: " << strTmp << std::endl;

    } else {
        int index = config.realMap[strTmp];
        int newHash = getHash(temp, 10);
        config.sendVec[newHash][index].value += val;
        std::cout << config.it->first << " yay found it " << config.it->second << std::endl;
        std::cout << "new value" << config.sendVec[newHash][index].value << std::endl;
    }
}

void distribute() {
    MPI_Request request[config.world_size];
	MPI_Status status[config.world_size];
    int counts[config.world_size] = {0};
    int blocksCount = 3;
    int blocksLength[3] = {11, 1, 1};
    MPI_Datatype types[3] = {MPI_CHAR, MPI_CHAR, MPI_INT};
    MPI_Aint offsets[3];
    offsets[0] = offsetof(Tuple, key);
    offsets[1] = offsetof(Tuple, padd);
    offsets[2] = offsetof(Tuple, value);
    MPI_Type_create_struct(blocksCount, blocksLength, offsets, types, &config.struct_type);
    MPI_Type_commit(&config.struct_type);

    for(int i = 0; i < config.sendVec.size(); i++) {
        if(i == config.world_rank) continue;
        MPI_Isend(&config.sendVec[i][0], config.sendVec[i].size(), config.struct_type, i, i, MPI_COMM_WORLD, &request[config.world_rank]);
        //MPI_Isend(&config.sendVec[i][0], config.sendVec[i].size()*16, MPI_CHAR, i, i, MPI_COMM_WORLD, &request[config.world_rank]);
    }
    for(int i = 0; i < config.sendVec.size(); i++) {
        if(i == config.world_rank) continue;
        MPI_Probe(i, config.world_rank, MPI_COMM_WORLD, &status[i]);
        //MPI_Get_count(&status[i], MPI_CHAR, &counts[i]);
        MPI_Get_count(&status[i], config.struct_type, &counts[i]);
        if(counts[i] != 0) {
            //std::vector<Tuple> temp(counts[i] / 16);
            std::vector<Tuple> temp(counts[i]);
            MPI_Recv(&temp[0], counts[i], config.struct_type, i, config.world_rank, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //MPI_Recv(&temp[0], counts[i], MPI_CHAR, i, config.world_rank, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //MPI_Irecv(void *buffer, int count, MPI_CHAR, i, config.world_rank, MPI_COMM_WORLD comm, MPI_STATUS_IGNORE);
            for(int j = 0; j < temp.size(); j++) {
                reduce(temp[j].key, temp[j].value);
            }
            /*for(int j = 0; j < (counts[i]); j++) {
                for(int k = 0; k < 10; k++) {
		            std::cout << temp[j].key[k];
                }
		        std::cout << std::endl << "temp vec value is: " << temp[j].value << std::endl;
            }*/
        }
    }
    if(config.world_rank == 2) {
        for(int i = 0; i < config.sendVec[config.world_rank].size(); i++) {
            for(int j = 0; j < 10; j++) {
                std::cout << config.sendVec[config.world_rank][i].key[j];
            }
            std::cout << " " << config.sendVec[config.world_rank][i].value << std::endl;
        }
    }
    return;
}