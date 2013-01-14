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
File: gpu-kmeans1D.cc
 - Kmeans with 2D input data on GPU.
Version: 0.20.1
***********************************************************************/

#include "stdint.h"

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

#include <cuda.h>
#include <cuda_runtime.h>

#include <iostream>
#include <cstdlib>
#include <cmath>

#include <time.h>
#include <sys/time.h>

//#define DEBUG

int deviceID = 0;

// datum of a plot
// x,y : coordinate
// cent : id of nearest cluster
class data {
public:
  float x;
  float y;
  int cent;
};

__device__ float mysqrt(data a, data b) {
  float x = abs(a.x - b.x);
  float y = abs(a.y - b.y);
  return std::sqrt(x*x + y*y);
}

//data object assignment
__global__ void assign_data(data *centroids,
			    data *data,
			    int num_of_data,
			    int num_of_cluster)
{
  int i;
  int tid = threadIdx.x + blockIdx.x * blockDim.x;
  //  int tid = threadIdx.x;
  //  int nthreads = blockDim.x;
  int nthreads = blockDim.x * gridDim.x;
  //  int part = num_of_data / nthreads; /* 65535*512 */
  //  for(i = part*tid; i < part*(tid+1); i++) {
  for (i = tid; i < num_of_data; i += nthreads) {
    int center = 0;
    float dmin = mysqrt(centroids[0], data[i]);
    for(int j = 1; j < num_of_cluster; j++) {
      float dist = mysqrt(centroids[j], data[i]);
      if(dist < dmin) {
	dmin = dist;
	center = j;
      }
    }
    data[i].cent = center;
  }
}

//K centroids recalculation
//tid has to be less than the num of newcent
__global__ void centroids_recalc(
           data *newcent,
	   data *d,
	   int *ndata) {
  int j;
  int tid = blockIdx.x;
  __shared__ float sx[64];
  __shared__ float sy[64];
  float x = 0.0f;
  float y = 0.0f;
  for(j = ndata[tid] + threadIdx.x; j < ndata[tid+1]; j += blockDim.x) {
    x += d[j].x;
    y += d[j].y;
  }
  sx[threadIdx.x] = x;
  sy[threadIdx.x] = y;
  __syncthreads();
  float n = static_cast<float>(ndata[tid+1]-ndata[tid]);

  if (threadIdx.x == 0) {
#pragma unroll
    for (j = 1; j < 64; j++) {
      x += sx[j];
      y += sy[j];
    }
    newcent[tid].x = x / n;
    newcent[tid].y = y / n;
  }
  /*
  int j;
  int tid = threadIdx.x;
  newcent[tid].x = 0.0;
  newcent[tid].y = 0.0;
  for(j = ndata[tid]; j < ndata[tid+1]; j++) {
    newcent[tid].x += d[j].x;
    newcent[tid].y += d[j].y;
  }
  float n = static_cast<float>(ndata[tid+1]-ndata[tid]);
  newcent[tid].x /= n;
  newcent[tid].y /= n;
  */
}


class KmeansMap: public HadoopPipes::Mapper {
public:
  KmeansMap(HadoopPipes::TaskContext& context){}
  
  double gettime() {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return tv.tv_sec+tv.tv_usec * 1e-6;
  }

  //zero init
  void init_int(int *data, int num) {
    for(int i = 0; i < num; i++) {
      data[i] = 0;
    }
  }
  void init_float(float *data, int num) {
    for(int i = 0; i < num; i++) {
      data[i] = 0.0;
    }
  }
  
  void sort_by_cent(data *d, int start, int end) 
  {
    int i = start;
    int j = end;
    int base = (d[start].cent + d[end].cent) / 2;
    while(1) {
      while (d[i].cent < base) i++;
      while (d[j].cent > base) j--;      
      if (i >= j) break;      
      data temp = d[i];
      d[i] = d[j];
      d[j] = temp;      
      i++;
      j--;
    }
    if (start < i-1) sort_by_cent(d, start, i-1);
    if (end > j+1)  sort_by_cent(d, j+1, end);
  } 

  //counts the nunber of data objects contained by each cluster   
  void count_data_in_cluster(data *d, int *ndata,
			     int num_of_data, int num_of_cluster) {
    int i;
    for(i = 0; i < num_of_data; i++) {
      ndata[d[i].cent + 1]++;
    }
    for(i = 1; i < num_of_cluster + 1; i++) {
      ndata[i] += ndata[i-1];
    }
  } 

  /*
  bool floatcmp(data *a, data *b, int num) {
    for(int i = 0; i < num; i++) {
      if(a[i].x != b[i].x || a[i].y != b[i].y) {
	return false;
      }
    }
    return true;
  }
  */
  float mysqrt(data a, data b) {
    float x = a.x - b.x;
    float y = a.y - b.y;
    return std::sqrt(x*x + y*y);
  }

  bool datacmp(data *a, data *b, int num) {
    for(int i = 0; i < num; i++) {
      if( mysqrt(a[i], b[i]) > 1 ) {
	return false;
      }
    }
    return true;
  }

  void map(HadoopPipes::MapContext& context) {
    // input format
    // --num of clusters ( == k)
    // --num of data( == n)
    // --initial centers for all clusters;
    // --input rows;

    //    fprintf(stderr, "start\n");

    int mp;

    double t[10];
    t[0] = gettime();

    std::vector<std::string> elements 
      = HadoopUtils::splitString(context.getInputValue(), " ");

    t[1] = gettime();

    const int k = HadoopUtils::toInt(elements[0]);
    const int n = HadoopUtils::toInt(elements[1]);
    // c[] : pos of cluster
    // d[] : data
    // ndata[] : num of data for each cluster
    data c[2][k];
    data d[n];
    int ndata[k+1];
    int i, cur, next, iter;

    //for Device
    data *dc;
    data *dd;
    int *dndata;

    //initialize
    for(i = 0; i < k; i++) {
      c[0][i].x = HadoopUtils::toFloat(elements[2*i+2]);
      c[0][i].y = HadoopUtils::toFloat(elements[2*i+3]);
    }
    for(i = 0; i < n; i++) {
      d[i].x = HadoopUtils::toFloat(elements[2*i+2*k+2]);
      d[i].y = HadoopUtils::toFloat(elements[2*i+2*k+3]);
    }    

    t[2] = gettime();

#ifdef DEBUG
    for(i = 0; i < k; i++) {
      std::cout << c[0][i].x << " " << c[0][i].y;
    }
    std::cout << '\n';
    for(i = 0; i < n; i++)
      std::cout << d[i].x << " " << d[i].y << " ";
    std::cout << '\n';
#endif

    //cuda init
    cudaDeviceProp prop;
    cudaGetDeviceProperties(&prop, 0);
    mp = prop.multiProcessorCount;
    cudaError_t err = cudaMalloc((void **)&dc, sizeof(data)*2*k);
    if(err != cudaSuccess) {
      std::cerr << "Malloc_1 failed: " << cudaGetErrorString(err) << ".\n";
    }
    err = cudaMalloc((void **)&dd, sizeof(data)*n);
    if(err != cudaSuccess) {
      std::cerr << "Malloc_2 failed: " << cudaGetErrorString(err) << ".\n";
    }
    err = cudaMalloc((void **)&dndata, sizeof(int)*(k+1));
    if(err != cudaSuccess) {
      std::cerr << "Malloc_3 failed: " << cudaGetErrorString(err) << ".\n";
    }

    t[3] = gettime();

    //    fprintf(stderr, "mid\n");


    // buffer id
    cur = 0;
    next = 1;
    iter = 0;
    do {
	iter++;
      //    for(int j = 0; j < 10; j++) {
      init_int(ndata, k+1);

      //data object assignment
      cudaMemcpy(dc + cur*k, c[cur], sizeof(data)*k, cudaMemcpyHostToDevice);
      cudaMemcpy(dd, d, sizeof(data)*n, cudaMemcpyHostToDevice);
      assign_data<<<mp,512>>>(dc + cur*k, dd, n, k);
      err = cudaMemcpy(d, dd, sizeof(data)*n, cudaMemcpyDeviceToHost);
      if(err != cudaSuccess) {
	  std::cerr << "Memcpy_1 failed: " << cudaGetErrorString(err) << ".\n";
      }
    
      t[4] = gettime();

#ifdef DEBUG
      for(i = 0; i < n; i++)
	std::cout << d[i].cent << " ";
      std::cout << '\n';
#endif

      //rearranges all data objects
      //and counts the nunber of data objects contained by each cluster 
      sort_by_cent(d, 0, n-1);
      count_data_in_cluster(d, ndata, n, k);

      t[5] = gettime();

#ifdef DEBUG
      for(i = 0; i < k+1; i++)
	std::cout << ndata[i] << " ";
      std::cout << '\n';
#endif
			  
      //K centroids recalculation
      err = cudaMemcpy(dndata, ndata, sizeof(int)*(k+1), cudaMemcpyHostToDevice);
      if(err != cudaSuccess) {
	  std::cerr << "Memcpy_2_1 failed: " << cudaGetErrorString(err) << ".\n";
      } else {
	  std::cerr << "Memcpy_2_1 success: " << cudaGetErrorString(err) << ".\n";
      }
      err = cudaMemcpy(dc + next*k, c[next], sizeof(data)*k, cudaMemcpyHostToDevice);
      if(err != cudaSuccess) {
	  std::cerr << "Memcpy_2_2 failed: " << cudaGetErrorString(err) << ".\n";
      } else {
	  std::cerr << "Memcpy_2_2 success: " << cudaGetErrorString(err) << ".\n";
      }
      err = cudaMemcpy(dd, d, sizeof(data)*n, cudaMemcpyHostToDevice);
      if(err != cudaSuccess) {
	  std::cerr << "Memcpy_2_3 failed: " << cudaGetErrorString(err) << ".\n";
      } else {
	  std::cerr << "Memcpy_2_3 success: " << cudaGetErrorString(err) << ".\n";
      }
      //      centroids_recalc<<<1,k>>>(dc + next*k, dd, dndata);
      centroids_recalc<<<k,64>>>(dc + next*k, dd, dndata);
      err = cudaMemcpy(c[next], dc + next*k, sizeof(data)*k, cudaMemcpyDeviceToHost);
      if(err != cudaSuccess) {
	  std::cerr << "Memcpy_2_4 failed: " << cudaGetErrorString(err) << ".\n";
      } else {
	  std::cerr << "Memcpy_2_4 success: " << cudaGetErrorString(err) << ".\n";
      }
      

      t[6] = gettime();


#ifdef DEBUG
      for(i = 0; i < k; i++)
	std::cout << c[next][i].x << " " << c[next][i].y << " ";
      std::cout << "\n\n";
#endif

      cur = 1 - cur;
      next = 1 - next;
    } while( datacmp(c[cur], c[next], k) == false && iter < 100);
      //    }

    //    fprintf(stderr, "finish\n");


    //emit 
    //key : cluster id
    //value : cluster centroid position
    for(i = 0; i < k; i++) {
      context.emit(context.getInputKey() + '\t' + HadoopUtils::toString(i),
		   HadoopUtils::toString((int)c[cur][i].x) + '\t' 
		   + HadoopUtils::toString((int)c[cur][i].y));
    }


    t[7] = gettime();

    std::cout << "Run on GPU" << '\n';
    std::cout << "iter : " << iter << '\n';
    for(i = 0; i < 7; i++) {
      std::cout << t[i+1] - t[i] << '\n';
    }
    std::cout <<  t[7] - t[0] << '\n';
    std::cout << '\n';

    cudaFree(dc);
    cudaFree(dd);
    cudaFree(dndata);
  }
};

class KmeansReduce: public HadoopPipes::Reducer {
public:
  KmeansReduce(HadoopPipes::TaskContext& context){}
  void reduce(HadoopPipes::ReduceContext& context) {
    while(context.nextValue()) {
      context.emit(context.getInputKey(), context.getInputValue());
    }
  }
};

int main(int argc, char *argv[]) {
  if(argc > 1) {
    deviceID = atoi(argv[1]);
    std::cout << "deviceID: " << deviceID << ".\n";
  }
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<KmeansMap,
                                                           KmeansReduce>());
}
