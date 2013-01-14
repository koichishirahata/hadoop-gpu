CC = g++
CPPFLAGS = -m64 -I$(HADOOP_HOME)/c++/Linux-amd64-64/include

cpu-submatmul: cpu-submatmul.cc
	$(CC) $(CPPFLAGS) $< -L$(HADOOP_HOME)/c++/Linux-amd64-64/lib -lhadooppipes -lhadooputils -lpthread -g -O2 -o $@

clean: 
	rm -rf cpu-submatmul