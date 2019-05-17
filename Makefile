CC = mpicc

FLAGS = -I./include -g -Wall -O3

all: main.out

run:
	mpirun -n 4 ./main.out ./testfiles/wikipedia_test_small.txt hejsan

main.out: src/main.c src/mapreduce.c
	$(CC) $(FLAGS) src/main.c -o main.out -lm

clean:
	rm -f *.o *.out