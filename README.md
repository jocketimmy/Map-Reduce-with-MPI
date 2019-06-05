# Map-Reduce-with-MPI

## Compile program

Use the gnu compiler:
module swap PrgEnv-current PrgEnv-gnu

load modules:
module add cdt/18.03
module swap craype craype/2.5.14
module swap cce cce/8.6.5
module swap cray-mpich cray-mpich/7.7.0

compile:
CC mainpp.cpp -o main.out -std=c++11

## Run program
aprun -n "processes" infile.txt outfile.txt
