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
File: cpu-kmeans1D.cc
 - Kmeans with 1D input data on CPU.
Version: 0.20.1
***********************************************************************/

#include  "stdint.h"

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

#include <iostream>
#include <cstdlib>

#include <time.h>
#include <sys/time.h>

//#define DEBUG

class KmeansMap: public HadoopPipes::Mapper {
public:
  KmeansMap(HadoopPipes::TaskContext& context){}
  
  // pos : coordinate
  // cent : id of nearest cluster
  class data {
  public:
    float pos;
    int cent;
  };

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
  
  // quick sort by d->cent
  void myqsort(data *d, int start, int end) 
  {
    int i = start;
    int j = end;
    float base = (d[start].cent + d[end].cent) / 2;
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
    if (start < i-1) myqsort(d, start, i-1);
    if (end > j+1)  myqsort(d, j+1, end);
  } 

  //data object assignment
  //calculate new centroid for each data(plot)
  void assign_data(
       float *centroids, data *data, int num_of_data, int num_of_cluster) {
    for(int i = 0; i < num_of_data; i++) {
      int center = 0;
      float dmin = abs(centroids[0] - data[i].pos);
      for(int j = 1; j < num_of_cluster; j++) {
	float dist = abs(centroids[j] - data[i].pos);
	if(dist < dmin) {
	  dmin = dist;
	  center = j;
	}
      }
      data[i].cent = center;
    }    
  }

  //counts the nunber of data objects contained by each cluster   
  void count_data_in_cluster(
       data *d, int *ndata, int num_of_data, int num_of_cluster) {
    int i;
    for(i = 0; i < num_of_data; i++) {
      ndata[d[i].cent]++;
    }
    //this may not be needed...
    for(i = 1; i < num_of_cluster; i++) {
      ndata[i] += ndata[i-1];
    }
  } 

  //K centroids recalculation
  void centroids_recalc(
       float *newcent, data *d, int *ndata, int num_of_data, int num_of_cluster) {
    int i, j;
    for(i = 0; i < num_of_cluster; i++) {
      newcent[i] = 0.0;
    }
    for(j = 0; j < ndata[0]; j++) {
      newcent[0] += d[j].pos;
    }
    newcent[0] /= static_cast<float>(ndata[0]);
    for(i = 1; i < num_of_cluster; i++) {
      for(j = ndata[i-1]; j < ndata[i]; j++) {
	newcent[i] += d[j].pos;
      }
      newcent[i] /= static_cast<float>(ndata[i]-ndata[i-1]);
    }
  }

  bool floatcmp(float *a, float *b, int num) {
    for(int i = 0; i < num; i++) {
      if(a[i] != b[i]) {
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
    std::vector<std::string> elements 
      = HadoopUtils::splitString(context.getInputValue(), " ");    
    const int k = HadoopUtils::toInt(elements[0]);
    const int n = HadoopUtils::toInt(elements[1]);
    // c[] : pos of cluster
    // d[] : data
    // ndata[] : num of data for each cluster
    float c[2][k];
    data d[n];
    int ndata[k];
    int i, j, cur, next;

    //initialize
    for(i = 0; i < k; i++) {
      c[0][i] = HadoopUtils::toFloat(elements[i+2]);
    }
    for(i = 0; i < n; i++) {
      d[i].pos = HadoopUtils::toFloat(elements[i+k+2]);
    }

#ifdef DEBUG
    for(i = 0; i < k; i++) {
      std::cout << c[0][i] << " ";
    }
    std::cout << '\n';
    for(i = 0; i < n; i++)
      std::cout << d[i].pos << " ";
    std::cout << '\n';
#endif

    // buffer id
    cur = 0;
    next = 1;

    for(int j = 0; j < 10; j++) {
    //    do {
      init_int(ndata, k);

      //data object assignment
      assign_data(c[cur], d, n, k);

#ifdef DEBUG
      for(i = 0; i < n; i++)
	std::cout << d[i].cent << " ";
      std::cout << '\n';
#endif
    
      //rearranges all data objects
      //and counts the nunber of data objects contained by each cluster 
      myqsort(d, 0, n-1);
      count_data_in_cluster(d, ndata, n, k);

#ifdef DEBUG
      for(i = 0; i < k; i++)
	std::cout << ndata[i] << " ";
      std::cout << '\n';
#endif
    
      //K centroids recalculation
      centroids_recalc(c[next], d, ndata, n, k);

#ifdef DEBUG
      for(i = 0; i < k; i++)
	std::cout << c[next][i] << " ";
      std::cout << "\n\n";
#endif

      cur = 1 - cur;
      next = 1 - next;
      //    } while( floatcmp(c[cur], c[next], k) == false );
    }

    //emit 
    //key : cluster id
    //value : cluster centroid position
    for(i = 0; i < k; i++) {
      context.emit(context.getInputKey() + "\t" + HadoopUtils::toString(i),
		   HadoopUtils::toString(c[cur][i]));
    }
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
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<KmeansMap,
                                                           KmeansReduce>());
}
