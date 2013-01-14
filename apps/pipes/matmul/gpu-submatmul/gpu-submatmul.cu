/***********************************************************************
 	hadoop-gpu
	Authors: Koichi Shirahata, Hitoshi Sato, Satoshi Matsuoka

This software is licensed under Apache License, Version 2.0 (the  "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-------------------------------------------------------------------------
File: gpu-submatmul.cc
 - Matrix multiplication on GPU using sub-matrix multiplication in Map stage.
Version: 0.20.1
***********************************************************************/

#include  "stdint.h"

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

#include <cuda.h>
#include <cuda_runtime.h>

#include <iostream>

#include <time.h>
#include <sys/time.h>

#define BLOCKSIZE (16)
#define GRIDSIZE (32)

__global__ void matmul(float *A, float *B, float *C, int m, int n)
{
  int i, j, k;
  float sum;
  i = blockIdx.y * blockDim.y + threadIdx.y;
  j = blockIdx.x * blockDim.x + threadIdx.x;
  sum = 0.0;
  for(k = 0; k < m; k++) {
    sum += A[i*m+k] * B[k*n+j];
  }
  C[i*n+j] = sum;
}

class MatmulMap: public HadoopPipes::Mapper {
public:
  MatmulMap(HadoopPipes::TaskContext& context){}

  double gettime() {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return tv.tv_sec+tv.tv_usec * 1e-6;
  }

  void map(HadoopPipes::MapContext& context) {

    //SubMatrix SA(m*m), SB(m*m), SC(m*m)
    //SA = A_ik
    //SB = B_kj
    //i: SA's row
    //j: SB's col
    //k: SA's col (== SB's row)
    //
    //each map compute SA*SB=SC
    //assume input format is like below
    //i j k m n sa(1,1) sa(1,2) ... sa(1,m) sa(2,1) ... sa(m,m) sb(1,1) ... sb(m,m)
    //
    //n: num of SA for one row
    // (total num of SA is n*n)
    //
    double t1, t2, t3, t4, t5, t6, t7, t8;
    t1 = gettime();
    std::string line = context.getInputValue();
    std::vector<std::string> elements = HadoopUtils::splitString(line, " ");    
    int i = HadoopUtils::toInt(elements[0]);
    int j = HadoopUtils::toInt(elements[1]);
    int k = HadoopUtils::toInt(elements[2]);
    int m = HadoopUtils::toInt(elements[3]);
    int n = HadoopUtils::toInt(elements[4]);
    int T = m*m;
    float SA[T], SB[T], SC[T];

    //variables for CUDA
    float *DSA, *DSB, *DSC;
    size_t array_size = sizeof(float) * T;

    t2 = gettime();
    std::string key = HadoopUtils::toString(i) 
      + " " + HadoopUtils::toString(j)
      + " " + HadoopUtils::toString(m)
      + " " + HadoopUtils::toString(n);
    
    for(k = 0; k < T; ++k) {
      SA[k] = HadoopUtils::toFloat(elements[k + 5]);
    }
    for(k = 0; k < T; ++k) {
      SB[k] = HadoopUtils::toFloat(elements[k + (5 + T)]);
    }    
    for(k = 0; k < T; k++) {
      SC[k] = 0.0;
    }

    t3 = gettime();
    cudaMalloc((void **)&DSA, array_size);
    cudaMalloc((void **)&DSB, array_size);
    cudaMalloc((void **)&DSC, array_size);        
    t4 = gettime();    
    cudaMemcpy(DSA, SA, array_size, cudaMemcpyHostToDevice);
    cudaMemcpy(DSB, SB, array_size, cudaMemcpyHostToDevice);
    t5 = gettime();

    //blockDim of grid should be specified properly
    matmul<<<dim3(GRIDSIZE, GRIDSIZE), dim3(BLOCKSIZE,BLOCKSIZE)>>>(DSA, DSB, DSC, m, m);
    t6 = gettime();    
    cudaMemcpy(SC, DSC, array_size, cudaMemcpyDeviceToHost);
    t7 = gettime();

    // output:
    // key -> (i, j, m, n)
    // value -> C_ijk (== SC)
    //
    std::string val_out = "";
    for(k = 0; k < T; ++k) {
      val_out += " ";
      val_out += HadoopUtils::toString(SC[k]);
    }

    //    std::cout << "key:" << key << ",val:" + val_out << "\n";
    context.emit(key, val_out);
    t8 = gettime();
    std::cout << t8-t1 << ", " << t2-t1 << ", " << t3-t2 << ", " << t4-t3 << ", " <<
	t5-t4 << ", " << t6-t5 << ", " << t7-t6 << ", " << t8-t7 << std::endl;

    // clean up memory
    cudaFree(DSA);
    cudaFree(DSB);
    cudaFree(DSC);
    cudaThreadExit();
  }
};

class MatmulReduce: public HadoopPipes::Reducer {
public:
  MatmulReduce(HadoopPipes::TaskContext& context){}
  void reduce(HadoopPipes::ReduceContext& context) {
    //
    // Sumup C_ijk (for each k)
    // 
    // input: key   -> (i,j, m, n) (assume C_ijk(m*m) = A_ik * B_kj)
    //        value -> list of C_ijk
    //
    // output: key -> element_id of TOTAL matrix
    //         value -> C_ijk[element_id]
    //
    std::string in_key = context.getInputKey();
    std::vector<std::string> keys = HadoopUtils::splitString(in_key, " ");

    std::string in_val;

    std::cout << in_key << '\n';
    int i = HadoopUtils::toInt(keys[0]);
    int j = HadoopUtils::toInt(keys[1]);
    int m = HadoopUtils::toInt(keys[2]);
    int n = HadoopUtils::toInt(keys[3]);
    int T = m*m;

    float C[T];
    int p = 0;
    for(p = 0; p < T; p++) {
      C[p] = 0.0;
    }

    //Sumup C_ijk[p] (for eack k, p)
    while(context.nextValue()) {
      in_val = context.getInputValue();
      std::cout << in_val << '\n';
      std::vector<std::string> vals = HadoopUtils::splitString(in_val, " ");
      for(p = 0; p < T; p++) {
    	C[p] += HadoopUtils::toFloat(vals[p]);
      }
    }
    
    //emit
    std::string key_out = "";
    for(p = 0; p < T; p++) {
      key_out = HadoopUtils::toString(m*i + p/m) + " "
    	+ HadoopUtils::toString(m*j + p%m);
      std::cout << "keyout:" << key_out << '\n';
      context.emit(key_out, HadoopUtils::toString(C[p]));
    }
  }
};

int main(int argc, char *argv[]) {
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<MatmulMap,
                                                           MatmulReduce>());
}
