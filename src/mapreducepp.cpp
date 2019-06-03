#include "../include/mapreduce.h"
#include <vector>
#include <iostream>
#include <unordered_map>
#include <string>
#include <stddef.h>

#define SEED_LENGTH 65
typedef uint64_t Hash;
const char           key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num         = (unsigned short*)key_seed;

//struct Tuple;
struct Tuple {
    char key[11] = {' '};
    char padd = 'x';
    int value;
};

struct Tup {
    char key[11];
    char padd = ':';
    char count = '7';
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
    char *receiveBuffer2;
    char *receiveBuffer3;
    char *last_buffer;

    // Ranks
    int world_rank, world_size;
    int MB = 64*1024*1024;
    MPI_Offset writeOffset = 0;

    std::unordered_map<std::string,int> realMap;
    std::unordered_map<std::string,int>::const_iterator it;
    std::vector<std::vector<Tuple> > sendVec;
    std::vector<Tup> printVec;

    MPI_Datatype struct_type;
};

struct Config config;

void handleInput(MPI_Offset);
void reduce(char*, int);
void collective_read(char*, char*);
void init(char*, char*);
void cleanup();
void mapReduce(char*);
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

    std::cout << "Did it work? " << config.sendVec[0].size() << " " << config.sendVec[1].size() << " " <<config.sendVec[2].size() <<" " << config.sendVec[3].size() << std::endl;

    return;
    //New stuff
    if(config.totalFileSize > 3*config.MB*config.world_size) {
        config.receiveBuffer2 = (char*) malloc(config.localFileCount * sizeof(char));
        config.receiveBuffer3 = (char*) malloc(config.localFileCount * sizeof(char));
        MPI_Request request[3];
        //MPI_Status status;
        int flags[3] = {-10};
        MPI_Offset offset = config.MB * sizeof(char) * config.world_size;
        int loopCount = (config.totalFileSize / offset) + 1;
        int i = 0;

        MPI_Offset disp = offset * i;
        MPI_File_iread_at(config.inFileHandler, disp + (config.chunk * config.world_rank), config.receiveBuffer, config.localFileCount, MPI_CHAR, &request[0]);
        i++;
        disp = offset * i;
        MPI_File_iread_at(config.inFileHandler, disp + (config.chunk * config.world_rank), config.receiveBuffer2, config.localFileCount, MPI_CHAR, &request[1]);
        i++;
        disp = offset * i;
        MPI_File_iread_at(config.inFileHandler, disp + (config.chunk * config.world_rank), config.receiveBuffer3, config.localFileCount, MPI_CHAR, &request[2]);
        i++;
        disp = offset * i;
        //if(disp + (config.chunk * config.world_rank) < config.totalFileSize) {

        while(i < loopCount) {
            MPI_Test(&request[0], &flags[0], MPI_STATUS_IGNORE);
            MPI_Test(&request[1], &flags[1], MPI_STATUS_IGNORE);
            MPI_Test(&request[2], &flags[2], MPI_STATUS_IGNORE);
            if(flags[0]) {
                mapReduce(config.receiveBuffer);
                //if(disp < config.totalFileSize) {
                    MPI_File_iread_at(config.inFileHandler, disp + (config.chunk * config.world_rank), config.receiveBuffer, config.localFileCount, MPI_CHAR, &request[0]);
                    i++;
                    disp = offset * i;
                    flags[0] = 0;
                /*}
                else {
                    //buffer1Done = true;
                }*/
            }
            if(flags[1]) {
                mapReduce(config.receiveBuffer2);
                MPI_File_iread_at(config.inFileHandler, disp + (config.chunk * config.world_rank), config.receiveBuffer2, config.localFileCount, MPI_CHAR, &request[1]);
                i++;
                disp = offset * i;
                flags[1] = 0;
            }
            if(flags[2]) {
                mapReduce(config.receiveBuffer3);
                MPI_File_iread_at(config.inFileHandler, disp + (config.chunk * config.world_rank), config.receiveBuffer3, config.localFileCount, MPI_CHAR, &request[2]);
                i++;
                disp = offset * i;
                flags[2] = 0;
            }
        }
    }
    std::cout << "Did it work? " << config.sendVec[0].size()+config.sendVec[1].size() + config.sendVec[2].size() << " " << config.sendVec[3].size() << std::endl;
    //MPI_File_close(&config.inFileHandler);
    return;
}

const int numDigits(int number) {
    int digits = 0;
    while (number) {
        number /= 10;
        digits++;
    }
    return digits;
}

void collective_write() {
    MPI_Request request;
    MPI_Offset tempOffset = 0;
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

    /*config.printVec.resize(config.sendVec[config.world_rank].size());
    for(int i = 0; i < config.printVec.size(); i++) {
        //std::cout << config.printVec.size() << " RANK " << config.world_rank << std::endl;
        int count = config.sendVec[config.world_rank][i].value;
        int countSize = numDigits(count);
        countSize++;
        char* temp = (char*) malloc(countSize*sizeof(char));
        snprintf(temp, countSize, "%d\n", count);
        std::cout << temp << std::endl;
        Tup tempTup;
        //tempTup.count = temp;
        for(int j = 0; j < 11; j++) {
            tempTup.key[j] = config.sendVec[config.world_rank][i].key[j];
        }
        config.printVec[i] = tempTup;
    }*/

    if(config.world_rank == 0) {
        //tempOffset = config.sendVec[config.world_rank].size();
        tempOffset = totalbytes;
        MPI_Isend(&tempOffset, 1, MPI_INT, config.world_rank + 1, 0, MPI_COMM_WORLD, &request);
    }
    else {
        MPI_Recv(&config.writeOffset, 1, MPI_INT, config.world_rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if(config.world_rank != config.world_size-1) {
            //tempOffset = config.writeOffset + config.sendVec[config.world_rank].size();
            tempOffset = config.writeOffset + totalbytes;
            MPI_Isend(&tempOffset, 1, MPI_INT, config.world_rank + 1, 0, MPI_COMM_WORLD, &request);
        }
    }
    //auto pointer = &config.sendVec[config.world_rank][0];
    std::cout << "Rank: " << config.world_rank << ", Has offset: " << config.writeOffset << ", Will write: " << config.sendVec[config.world_rank].size() << std::endl;
	MPI_File_open(MPI_COMM_WORLD, config.outFile, (MPI_MODE_RDWR | MPI_MODE_CREATE), MPI_INFO_NULL, &config.outFileHandler);
    //if(config.world_rank == 3) {
    //MPI_File_write_at(config.outFileHandler, config.writeOffset*14*sizeof(char), &print[0], config.sendVec[config.world_rank].size()*14, MPI_CHAR, MPI_STATUS_IGNORE);
    MPI_File_write_at(config.outFileHandler, config.writeOffset*sizeof(char), &print[0], totalbytes, MPI_CHAR, MPI_STATUS_IGNORE);
    //REAL MPI_File_write_at(config.outFileHandler, config.writeOffset*16*sizeof(char), &config.sendVec[config.world_rank][0], config.sendVec[config.world_rank].size()*16, MPI_CHAR, MPI_STATUS_IGNORE);
    //MPI_File_write_at(config.outFileHandler, config.writeOffset*16*sizeof(char), &config.sendVec[config.world_rank][0], config.sendVec[config.world_rank].size()*4, MPI_INT, MPI_STATUS_IGNORE);
    //MPI_File_write_at(config.outFileHandler, config.writeOffset*16*sizeof(char), &config.sendVec[config.world_rank][0], config.sendVec[config.world_rank].size(), config.struct_type, MPI_STATUS_IGNORE);

    //}
}

void handleInput(MPI_Offset offset) {
    int count = 0;
    if(offset > config.totalFileSize-config.chunk) {
        config.last_buffer = (char*) malloc(config.totalFileSize % config.chunk);
        config.receiveBuffer = config.last_buffer;
        count = (config.totalFileSize % config.chunk) / sizeof(char);
        config.localFileCount = count;
    }
    std::cout << "READ A CHUNK HERE" << std::endl;
    MPI_File_read_at(config.inFileHandler, offset, config.receiveBuffer, config.localFileCount, MPI_CHAR, MPI_STATUS_IGNORE);

    mapReduce(config.receiveBuffer);
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


void mapReduce(char* receiveBuffer) {
    const char space[30] = "-=%?&•!·_,.'<>/:;\" \n\t";
    char* token;
    //int in = 0;
    //std::vector<char*> buffer;
    token = strtok(receiveBuffer, space);
    while(token != NULL){
        int looop = 0;
        while(strlen(token)-looop > 10) {
            char* newToken = (char*) malloc(11*sizeof(char));
            for(int i = 0+looop; i < 10+looop; i++) {
                newToken[i-looop] = token[i];
            }
            newToken[10] = 0;
            reduce(newToken, 1);
            looop += 10;
        }
        if(strlen(token)-looop > 0) {
            char* lastToken = (char*) malloc(11*sizeof(char));
            for(int i = 0; i < strlen(token)-looop; i++) {
                lastToken[i] = token[i+looop];
            }
            for(int i = strlen(token)-looop; i < 10; i++) {
                lastToken[i] = ' ';
            }
            lastToken[10] = 0;
            reduce(lastToken, 1);
        }
        /*
        char tempChar[10];
        int loop = 0;
        if(strlen(token) > 10) {
            for(int i = 0; i < strlen(token); i++){
                if(i % 10 == 0){
                    reduce(tempChar);
                    for(int j = 0; j < 10; j++){
                        tempChar[j] = ' ';
                    }
                    loop++;
                }
                tempChar[i - 10*loop] = token[i];
            }
            reduce(token);
        }else{
            reduce(token);
        }*/
        token = strtok(NULL, space);
    }

    /*int localFileSize = (config.localFileCount / 10) + 1;
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
    }*/
    return;
}

void reduce(char* temp, int val) {
    std::string strTmp(temp);
    config.it = config.realMap.find(strTmp);
    if(config.it == config.realMap.end()) {
        int newHash = getHash(temp, 10);
        /*Tuple tempTuple;
        for(int k = 0; k < 11; k++) {
            tempTuple.key[k] = temp[k];
        }*/
        //sprintf(tempTuple.value, "%s%d", tempTuple.value, val);
        //std::cout << tempTuple.value << " " << strlen(tempTuple.value) << std::endl;
        //tempTuple.value = val;
        config.sendVec[newHash].emplace_back(Tuple());
        //config.sendVec[newHash][config.sendVec[newHash].size()-1].key = temp;
        for(int k = 0; k < 11; k++) {
            config.sendVec[newHash][config.sendVec[newHash].size()-1].key[k] = temp[k];
        }
        config.sendVec[newHash][config.sendVec[newHash].size()-1].value = val;
        config.realMap.insert({strTmp, config.sendVec[newHash].size()-1});
        //std::cout << config.sendVec[newHash][config.sendVec[newHash].size()-1].value << " NO you did not found it, you wanted: " << strTmp << std::endl;

    } else {
        int index = config.realMap[strTmp];
        int newHash = getHash(temp, 10);
        //int timmy = std::atoi(config.sendVec[newHash][index].value);
        //int timmy += 1;
        //config.sendVec[newHash][index].value;
        config.sendVec[newHash][index].value += val;
        //std::cout << config.it->first << " yay found it " << config.it->second << std::endl;
        //std::cout << "new value" << config.sendVec[newHash][index].value << std::endl;
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
        std::cout << "RECEIVE DONE" << std::endl;

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
    /*if(config.world_rank == 2) {
        for(int i = 0; i < config.sendVec[config.world_rank].size(); i++) {
            for(int j = 0; j < 10; j++) {
                std::cout << config.sendVec[config.world_rank][i].key[j];
            }
            std::cout << " " << config.sendVec[config.world_rank][i].value << std::endl;
        }
    }*/
    std::cout << "REDUCE DONE" << std::endl;

    return;
}