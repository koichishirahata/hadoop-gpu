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
File: cpu-matmul.cc
 - Plain matrix multiplication on CPU.
Version: 0.20.1
***********************************************************************/

#include  "stdint.h"

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

#include <iostream>

#include <time.h>
#include <sys/time.h>

class MatmulMap: public HadoopPipes::Mapper {
public:
  MatmulMap(HadoopPipes::TaskContext& context){}

  double gettime() {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return tv.tv_sec+tv.tv_usec * 1e-6;
  }

  void map(HadoopPipes::MapContext& context) {
    double st, ft;
    st = gettime();
    
    //split strings by line
    std::string line = context.getInputValue();

    //only first roop is out of for roop
    std::vector<std::string> elements = HadoopUtils::splitString(line, " ");
    int i = HadoopUtils::toFloat(elements[0]);
    int j = HadoopUtils::toFloat(elements[1]);
    int T = (elements.size()-2) / 2;
    float a[T], b[T];
    int k;
    for(k = 0; k < T; ++k) {
      a[k] = HadoopUtils::toFloat(elements[k + 2]);
    }
    for(k = 0; k < T; ++k) {
      b[k] = HadoopUtils::toFloat(elements[k + (T + 2)]);
    }
    std::string key = HadoopUtils::toString(i) + " " + HadoopUtils::toString(j);

    //assign keys and values
    //key: (i, j) ("ij")
    //value: a(i,k)*b(k*j) (k=0 -> k<size())
    for(k = 0; k < T; ++k) {
      context.emit(key,HadoopUtils::toString(a[k]*b[k]));
    }
    ft = gettime();
    std::cout << ft-st << ", " << key << std::endl;
  }
};

class MatmulReduce: public HadoopPipes::Reducer {
public:
  MatmulReduce(HadoopPipes::TaskContext& context){}
  void reduce(HadoopPipes::ReduceContext& context) {
    // sumup values which have the same keys
    float sum = 0;
    while (context.nextValue()) {
      sum += HadoopUtils::toFloat(context.getInputValue());
    }
    context.emit(context.getInputKey(), HadoopUtils::toString(sum));
  }
};

int main(int argc, char *argv[]) {
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<MatmulMap,
                                                           MatmulReduce>());
}
