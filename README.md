# External Sort

This program implements an external merge sort algorithm, which is used to sort data that is larger than the amount of memory available for sorting.

The algorithm works as follows, given a buffer pool size of B pages and a page size of P in bytes:

**Pass 0**: Split the input files into runs of size P. A total of R runs are produced, are sorted in memory by Quicksort and are written out to disk individually.

**Pass 1 - Pass log<sub>k</sub>(R)**:
For each of the remaining passes, we merge k (where k = B - 1) runs at time and write the merged run to disk. We use k buffers for input and use the k+1th buffer as our output buffer. The final pass produces one run, which is fully sorted.

## Optimizations

### Reduce the number of initial runs
One way to create the initial runs would be to split up the file by its natural sort order. For example "5, 8, 9, 1, 2", would be split into into two runs: R1 = "5, 8, 9" and R2 = "1, 2".

However, by producing runs of a fixed size, P, we reduce the number of initial runs, and in turn, reduce the number of merge passes.

### Perform k-way merges

Rather than merging runs in pairs during each pass, we could use as many buffers as possible, and thus merge k runs at a time. This will also reduce the total number of passes of the algorithm.

### Do not write the final run to disk (not implemented)

A further optimization would be to not write the final run out to disk. Instead, during the last pass, we could simply perform the k-way merge and stream the final run directly to STDOUT, which would save some IOs.

## Usage
```
Usage: java -jar externalSort.jar [--help] [-n=numBuffers] [-p=pageSize] FILE
  FILE                    The file to sort
      --help                  display this help message
  -n, --numBuffers=numBuffers The number of buffers in the buffer pool
  -p, --pageSize=pageSize     The page size in bytes
  ``` 


