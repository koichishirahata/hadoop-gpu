#!/bin/bash

CMDNAME=`basename $0`
if [ $# -ne 1 ]; then
    echo "Usage: $CMDNAME input" 1>&2
    exit 1
fi

bin/hadoop dfs -rmr output

bin/hadoop accels -D hadoop.pipes.java.recordreader=true\
 -D hadoop.pipes.java.recordwriter=true\
 -output output\
 -cpubin bin/gpu-kmeans1D\
 -gpubin bin/gpu-kmeans1D\
 # -D mapred.tasktracker.map.cpu.tasks.maximum=1\
 # -D mapred.tasktracker.map.gpu.tasks.maximum=0\
 -input $1
