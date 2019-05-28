#include "../include/mapreduce.h"
#include <vector>
#include <iostream>
#include <unordered_map>
#include <string>

#define SEED_LENGTH 65
typedef uint64_t Hash;

const char           key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num         = (unsigned short*)key_seed;

struct Tuple;

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

    std::vector<std::vector<Tuple> > sendVec;

    MPI_Datatype struct_type;

};

struct Config config;

void handleInput(MPI_Offset offset);

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
    char key[11];
    char pad = 'x';
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
    //char temp[11];
    //std::string strTmpz(temp);

    for(int i = 0; i < localFileSize; i++) {
        char* temp = (char*) malloc(11*sizeof(char));

        //std::cout << "loop" << i << std::endl;

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

        //strTmpz = strTmp;
        it = config.realMap.find(strTmp);

        if(it == config.realMap.end()) {

            //std::cout << "not found, you were looking for " << strTmp << std::endl;
           // config.realMap.insert({strTmp, 1});
            Hash hash = getHash(temp, 10);
            int newHash = (int) (hash % config.world_size);
            //std::cout << "RANK " << config.world_rank << " " << newHash << " " << config.sendVec[newHash].size() << std::endl;

            Tuple tempTuple;
            for(int k = 0; k < 11; k++) {
                tempTuple.key[k] = temp[k];
            }
            //tempTuple.key = temp;
            tempTuple.value = 1;
            config.sendVec[newHash].push_back(tempTuple);

            //config.sendVec[newHash][config.sendVec[newHash].size()-1].key = temp;
            //config.sendVec[newHash][config.sendVec[newHash].size()-1].value = 1;
            config.realMap.insert({strTmp, config.sendVec[newHash].size()-1});
        } else {
            //std::cout << "found" << std::endl;

            //config.realMap[strTmp] += 1;
            //std::cout << it->first << " yay found it " << it->second << std::endl;
            int index = config.realMap[strTmp];
            Hash hash = getHash(temp, 10);
            int newHash = (int) (hash % config.world_size);
            config.sendVec[newHash][index].value += 1;
            //std::cout << "new value" << config.sendVec[newHash][index].value << std::endl;

        }
    }
    //std::cout << strTmpz << std::endl;
    /*for(int i = 0; i < config.sendVec.size(); i++) {
        std::cout << "rank " << i << std::endl;
        for(int j = 0; j < config.sendVec[i].size(); j++) {
            for(int k = 0; k < 10; k++) {
                std::cout << config.sendVec[i][j].key[k];
            }
            std::cout << " value: " << config.sendVec[i][j].value << std::endl;
        }
    }*/


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

struct Tup {
    char key[11];
    char padd = 'x';
    //char padd2 = 'x';
    //char padd3 = 'x';
    int value;
};

void distribute() {
	std::cout << "world size: " << config.world_size << std::endl;

    MPI_Request request[config.world_size];
	MPI_Status status[config.world_size];
    int flags[config.world_size];
    int counts[config.world_size] = {0};
    int arr[10] = {7};
    int receive[4];
	std::cout << "rank is: " << config.world_rank << std::endl;

    int blocksCount = 3;
    int blocksLength[3] = {11, 1, 1};
    MPI_Datatype types[3] = {MPI_CHAR, MPI_CHAR, MPI_INT};
    MPI_Aint offsets[3];
    offsets[0] = offsetof(Tup, key);
    offsets[1] = offsetof(Tup, padd);
    offsets[2] = offsetof(Tup, value);
    //offsets[3] = offsetof(Tup, padd3);
    //offsets[4] = offsetof(Tup, value);

    MPI_Type_create_struct(blocksCount, blocksLength, offsets, types, &config.struct_type);
    MPI_Type_commit(&config.struct_type);


    for(int i = 0; i < config.sendVec.size(); i++) {
        MPI_Isend(&config.sendVec[i][0], config.sendVec[i].size(), config.struct_type, i, i, MPI_COMM_WORLD, &request[config.world_rank]);

        //MPI_Isend(&config.sendVec[i][0], config.sendVec[i].size()*16, MPI_CHAR, i, i, MPI_COMM_WORLD, &request[config.world_rank]);
        //MPI_Iprobe(i, config.world_rank, MPI_COMM_WORLD, &flags[i], &status[i]);
    }
    for(int i = 0; i < config.sendVec.size(); i++) {
        MPI_Probe(i, config.world_rank, MPI_COMM_WORLD, &status[i]);
        MPI_Get_count(&status[i], MPI_CHAR, &counts[i]);
        MPI_Get_count(&status[i], config.struct_type, &counts[i]);

        std::cout << config.world_rank << ", loop " << i << " count is " << counts[i] << std::endl;
        if(counts[i] != 0 && config.world_rank == 0) {
        	std::cout << "count is " << counts[i] << std::endl;
            std::cout << "i is " << i << std::endl;

            //std::vector<Tup> temp(counts[i] / 16);
            std::vector<Tup> temp(counts[i]);

            MPI_Recv(&temp[0], counts[i], config.struct_type, i, config.world_rank, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		    std::cout << "after receive" << std::endl;

            //MPI_Recv(&temp[0], counts[i], MPI_CHAR, i, config.world_rank, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		    //std::cout << "temp vec value is" << temp[0].value << std::endl;

            for(int j = 0; j < (counts[i]); j++) {
                for(int k = 0; k < 10; k++) {
		            std::cout << temp[j].key[k];
                }
                char* temp2 = (char*) malloc(2*sizeof(char));
                temp2[0] = 'T';
                temp2[1] = 0;
                //temp[j].key = temp2;
		        std::cout << std::endl << "temp vec value is: " << temp[j].value << std::endl;
                //std::cout << temp[j].key[0];
            }

            //MPI_Irecv(void *buffer, int count, MPI_CHAR, i, config.world_rank, MPI_COMM_WORLD comm, MPI_STATUS_IGNORE);
        }
    }
    /*for(int i = 0; i < config.sendVec.size(); i++) {
        //MPI_Probe(i, i, MPI_COMM_WORLD, &status[i]);
		std::cout << "flags here: " << flags[i] << std::endl;
    }*/
    return;
}