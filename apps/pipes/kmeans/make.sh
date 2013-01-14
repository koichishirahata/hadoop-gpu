#!/bin/bash

cd cpu-kmeans1D
make
cd ..

cd gpu-kmeans1D
make
cd ..

cd cpu-kmeans2D
make 
cd ..

cd gpu-kmeans2D
make 
cd ..