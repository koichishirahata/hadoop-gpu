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
 -cpubin bin/gpu-kmeans2D\
 -gpubin bin/gpu-kmeans2D\
 -input $1
