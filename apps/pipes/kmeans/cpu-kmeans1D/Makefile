CC = g++
CPPFLAGS = -m64 -I$(HADOOP_HOME)/c++/Linux-amd64-64/include

cpu-kmeans1D: cpu-kmeans1D.cc
	$(CC) $(CPPFLAGS) $< -L$(HADOOP_HOME)/c++/Linux-amd64-64/lib -lhadooppipes -lhadooputils -lpthread -g -O2 -o $@

clean:
	rm -f cpu-kmeans1D
