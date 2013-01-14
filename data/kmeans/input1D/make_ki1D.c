/* make_ki.c */
/*
 * Make kmeans input file
 * whose cluster centroids and posions of data will be made at ramdom.
 * 
 * arguments
 * 1: k; the number of centroids 
 * 2: n; the number of data
 * 3: numiter; the number of dataset(which include k, n, and data for each line)
 * 4: fname; output file name (which contains k, n, and data, for kmeans input)
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/* the maximum of random number which goes up to */
#define MAX 100

int main(int argc, char *argv[]) {
  srand((unsigned) time(NULL));

  int k, n, numiter, iter;
  FILE *fp;
  char *fname = NULL;
  const float m = 1.0 / (RAND_MAX + 1.0);

  if(argc > 4) {
    k = atoi(argv[1]);
    n = atoi(argv[2]);
    numiter = atoi(argv[3]);
    fname = argv[4];
    if((fp = fopen(fname, "w")) < 0) {
      error("Can't open %s", fname);
    }
    
    for(iter = 0; iter < numiter; iter++) {  
      int i;
      float x;
      fprintf(fp, "%d %d ", k, n);
      /* for cluster centroids */
      for(i = 0; i < k; i++) {
	x = m * rand() * MAX;
	fprintf(fp, "%f ", x);
      }
      /* for positions of data */
      for(i = 0; i < n; i++) {
	x = m * rand() * MAX;
	fprintf(fp, "%f ", x);
      }
      fprintf(fp, "\n");
    }    
  }
  else {
    puts("arguments must be included below");
    puts("1: k; the number of centroids");
    puts("2: n; the number of data");
    puts("3: numiter; the number of dataset(which include k, n, and data for each line)");
    puts("4: fname; output file name (which contains k, n, and data, for kmeans input)");
  }  
  
}
