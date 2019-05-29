#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <mpi.h>
#include "mapreducepp.cpp"


void print_usage(char *program);

int main(int argc, char *argv[]) {
	int opt;
	int world_rank;
	int repeat = 1;
	void (*algorithm)() = &mapReduce;

	double avg_runtime = 0.0, prev_avg_runtime = 0.0, stddev_runtime = 0.0;
	double start_time, end_time;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

	while ((opt = getopt(argc, argv, "cfr:")) != -1) {
		switch (opt) {
			case 'f':
				if (world_rank == 0) fprintf(stderr, "Hard work work\n");
				algorithm = &mapReduce;
				break;
			case 'r':
				repeat = atoi(optarg);
				break;
			default:
				if (world_rank == 0) print_usage(argv[0]);
				MPI_Finalize();
				exit(1);
		}
	}

	if (argv[optind] == NULL || argv[optind + 1] == NULL) {
		if (world_rank == 0) print_usage(argv[0]);
		MPI_Finalize();
		exit(1);
	}

	//init(argv[optind], argv[optind + 1]);
	collective_read(argv[optind], argv[optind + 1]);
    MPI_Barrier(MPI_COMM_WORLD);
	distribute();
    MPI_Barrier(MPI_COMM_WORLD);
	collective_write();
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

	for (int i = 0; i < repeat; i++) {
		MPI_Barrier(MPI_COMM_WORLD);
		start_time = MPI_Wtime();
		//algorithm();
		MPI_Barrier(MPI_COMM_WORLD);
		end_time = MPI_Wtime();

		if (world_rank == 0) {
			printf("run %d: %f s\n", i, end_time - start_time);
		}
		prev_avg_runtime = avg_runtime;
		avg_runtime = avg_runtime + ( (end_time - start_time) - avg_runtime ) / (i + 1);
		stddev_runtime = stddev_runtime + ( (end_time - start_time) - avg_runtime) * ( (end_time - start_time) - prev_avg_runtime);
	}

	if (world_rank == 0) {
		stddev_runtime = sqrt(stddev_runtime / (repeat - 1));
		printf("duration\t= %f±%f\n", avg_runtime, stddev_runtime);
	}

	//Put cleanup func here
	MPI_Finalize();

	return 0;
}

void print_usage(char *program) {
	fprintf(stderr, "Usage: %s [Inputfile] [Outputfile]\n", program);
}
