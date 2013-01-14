CC = nvcc
CPPFLAGS = -m64 -I$(HADOOP_HOME)/c++/Linux-amd64-64/include

gpu-submatmul: gpu-submatmul.cu
	$(CC) $(CPPFLAGS) $< -L$(HADOOP_HOME)/c++/Linux-amd64-64/lib -lhadooppipes -lhadooputils -lpthread -g -O2 -o $@

clean:
	rm -rf gpu-submatmul