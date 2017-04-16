<b>Page Rank Algorithms</b>

This project runs thee separate PageRank algorithms on .csv files. The files must be in the form where each line contains node1Name,node1Value,node2Name,node2Value. Three types of PageRank algorithms are then run on the network, an iterative-based method, a matrix-based method, and a parallelized (multithreaded) iterative-based method. The matrix-based method also has a separate class for a BigDecimal version, as the original only uses the double data structure and can have rounding errors for very large networks (note: the BigDecimal version removes rounding errors but at the significant cost of runtime).

<b>Classes</b>

- Atomic Node: This class stores relevant values for the PageRank algorithms. Specifically the atomic node class is used in the parallelized iterative-based PageRank algorithm. Due to the multithreaded nature of the algorithm, the atomic node class ensures accurate values for the nodes even through concurrent access by multiple threads.
- Node: This class stores relevant values for the PageRank algorithms. This class is used in all of the algorithms except the parallelized version, as each node does not ensure accurate update values in a multithreaded system.
- PageRank: This class runs all three PageRank algorithms – the iterative-based, regular matrix-based, and the parallelized iterative-based. It will run the algorithms on any series of inputted .csv files and will print the results out to newly created test.csv files.
- PageRankBigDecimal: This class runs the BigDecimal version of the matrix-based PageRank algorithm.
- PartitionPageRank: This class runs the parallelized iterative-based PageRank algorithm. It makes use of the PartitionThread to run the traditional iterative-based algorithm on each partitioned section of the original network.
- PartitionThread: This class overrides the run method of the standard Thread class and runs the iterative-based PageRank algorithm on a smaller partition of the original network.
