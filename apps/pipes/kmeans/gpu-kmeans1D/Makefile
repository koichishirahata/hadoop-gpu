CC = nvcc
CPPFLAGS = -m64 -I$(HADOOP_HOME)/c++/Linux-amd64-64/include

gpu-kmeans1D: gpu-kmeans1D.cu
	$(CC) $(CPPFLAGS) $< -L$(HADOOP_HOME)/c++/Linux-amd64-64/lib -lhadooppipes -lhadooputils -lpthread -g -O2 -o $@

clean:
	rm -f gpu-kmeans1D

