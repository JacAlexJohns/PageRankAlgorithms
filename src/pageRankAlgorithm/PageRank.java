package pageRankAlgorithm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class PageRank {
	
	/*
	 * Input: Any number of filepaths linking to .csv files
	 * Output: The runtime for each of the three pagerank algorithms
	 * This also prints the results for the three algorithms to .csv files
	 * The following program runs three pagerank algorithms: an iterative version,
	 * a matrix version, and a parallelized-iterative version on given .csv files.
	 */
	public static void main(String[] args) throws Exception {
		
		// An accumulator to write to different files for each input file
		int accumulator = 1;
		System.out.println("Input Filenames: ");
		// Read in input file names
		Scanner sc = new Scanner(System.in);
		while (sc.hasNextLine()) {
			// Only valid file names will be accepted, otherwise an exception will be thrown
			String fileName = sc.nextLine().trim();
			System.out.println("\n" + fileName);
			if (!fileName.equals("")) {
				// Run the pagerank algorithms on the files
				runAndWriteAll(fileName, "E://Social Computing/test" + accumulator++ + ".csv");
			}
		} sc.close();
		
	}
	
	/*
	 *  A test setup to analyze the page rank algorithm on a simple network
	 */
	public static List<Node> testSetup() {
		List<Node> network = new ArrayList<Node>();
		
		Node a = new Node("A");
		Node b = new Node("B");
		Node c = new Node("C");
		Node d = new Node("D");
		Node e = new Node("E");
		
		a.addEdge(b); 
		b.addEdge(e);
		c.addEdge(a); c.addEdge(b); c.addEdge(d); c.addEdge(e);
		d.addEdge(c); d.addEdge(e);
		e.addEdge(d);

		a.setValue(3); b.setValue(4); c.setValue(5); d.setValue(7); e.setValue(1);
		
		network.add(a); network.add(b); network.add(c); network.add(d); network.add(e);
		initializeValues(network);
		return network;
	}
	
	/*
	 * This method runs the iterative page rank algorithm on a given file.
	 * It also prints out the running time of the algorithm in milliseconds.
	 */
	public static List<Node> runIterative(String fileName) throws Exception {
		// Build the network on which the pagerank algorithm will be run
		List<Node> network = buildNetwork(fileName);
		// Calculate the variance which will determine when the algorithm converges
		double variance = calculateVariance(network.size());
		
		System.out.println("Iterative:");
		final long startTime = System.currentTimeMillis();
		// Run the pagerank algorithm for the network using the calculated variance
		network = pageRankIterativeScaled(network, variance);
		final long endTime = System.currentTimeMillis();
		System.out.println("Run Time: " + ((double)endTime - startTime) + " ms");
		
		return network;
	}
	
	/*
	 * This method runs the matrix page rank algorithm on a given file.
	 * It also prints out the running time of the algorithm in milliseconds.
	 */
	public static List<Node> runMatrix(String fileName) throws Exception {
		// Build the network on which the pagerank algorithm will be run
		List<Node> network = buildNetwork(fileName);
		// Calculate the variance which will determine when the algorithm converges
		double variance = calculateVariance(network.size());
		
		System.out.println("Random Walk:");
		final long startTime = System.currentTimeMillis();
		// Run the pagerank algorithm for the network using the calculated variance
		network = pageRankRandomWalkScaled(network, variance);
		final long endTime = System.currentTimeMillis();
		System.out.println("Run Time: " + ((double)endTime - startTime) + " ms");
		
		return network;
	}
	
	/*
	 * This method runs all three of the pagerank algorithms on the given .csv file and
	 * prints the results to a new .csv file.
	 */
	public static void runAndWriteAll(String readFileName, String writeFileName)  throws Exception {
		
		// Create the writer that will send the text string to the new file
		PrintWriter pw = new PrintWriter(new File(writeFileName));
		// Create a string builder to accumulate the text for the new file
        StringBuilder sb = new StringBuilder();
        // Append the titles for the data first
        sb.append(" ,Iterative, ,Matrix, ,Parallelized, \n");
        sb.append("Rank,Name,Value,Name,Value,Name,Value\n");
        
        // Generate the network results of the pagerank algorithms
		List<Node> network1 = runIterative(readFileName);	
		network1 = sort(network1);
		List<Node> network2 = runMatrix(readFileName);
		network2 = sort(network2);
		List<AtomicNode> network3 = PartitionPageRank.runParallelized(readFileName);
		network3 = PartitionPageRank.sort(network3);
		
		// For each node in order of rank print its name and value for each of the pagerank algorithms
		for (int i = 0; i < network1.size(); i++) {
			sb.append(i+1 + ",");
			sb.append(network1.get(i).getName() + "," + network1.get(i).getValue() + ",");
			sb.append(network2.get(i).getName() + "," + network2.get(i).getValue() + ",");
			sb.append(network3.get(i).getName() + "," + network3.get(i).getValue() + "\n");
		}
		
		// Write the resulting string to the file
		pw.write(sb.toString());
		pw.close();
	}
	
	/*
	 * This method tests the three pagerank algorithms to assess whether or not they generate
	 * the same results. (NOTE: the matrix version of the pagerank algorithm occasionally gives
	 * slightly different results for very large networks due to rounding error using the java
	 * double. This can be remedied by using the BigDecimal data structure, which can be found in
	 * the PageRankBigDecimal.java file, however it is at the significant cost of the runtime.) 
	 */
	public static void testIterativeVsMatrixVsParallelized(String fileName) throws Exception {
		
		// Runs the three pagerank algorithms and sorts each of them
		List<Node> network1 = runIterative(fileName);	
		network1 = sort(network1);
		List<Node> network2 = runMatrix(fileName);
		network2 = sort(network2);
		List<AtomicNode> network3 = PartitionPageRank.runParallelized(fileName);
		network3 = PartitionPageRank.sort(network3);
		
		// Goes through the networks and checks if any of the nodes are not in the same rank in a different network
		for (int i = 0; i < network1.size(); i++) {
			if (!(network1.get(i).equals(network2.get(i)) & network1.get(i).equals(network3.get(i)))) {
				System.out.println(network1.get(i).getName() + " : " + network2.get(i).getName() + " : " + network3.get(i).getName());
				System.out.printf("%.30f : %.30f : %.30f\n", network1.get(i).getValue(), network2.get(i).getValue(), network3.get(i).getValue());
			}
		}
	}
	
	/*
	 * This method takes in a filename and builds the network that will be used
	 * in the pagerank algorithms. (NOTE: it is assumed that the file is a .csv
	 * file and that it is undirected. A directed network can still be created
	 * if the .csv file has an edge going in both directions towards every pair
	 * of connected nodes.)
	 */
	@SuppressWarnings("resource")
	public static List<Node> buildNetwork(String fileName) throws Exception {
		// A BufferedReader to read in the .csv file
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		
		String line;
		List<Node> network = new ArrayList<Node>();
		
		// For each line in the .csv file, build an edge from the first node to the second
		while ((line = br.readLine()) != null) {
			String[] lineItems = line.split(",");
			// Create the from and to nodes
			Node nodeFrom = new Node(lineItems[0]);
			Node nodeTo = new Node(lineItems[2]);
			// Check if the network already contains either nodes
			if (network.contains(nodeFrom)) nodeFrom = network.get(network.indexOf(nodeFrom));
			else network.add(nodeFrom);
			if (network.contains(nodeTo)) nodeTo = network.get(network.indexOf(nodeTo));
			else network.add(nodeTo);
			// Create an edge from the nodeFrom to the nodeTo
			nodeFrom.addEdge(nodeTo);
		}
		
		// Fix all network sinks
		fixNetworkSinks(network);
		// Initialize the values of the network
		initializeValues(network);
		
		return network;
	}
	
	/*
	 * This method calculates the variance of the network.
	 * The variance is used to check for convergence in the network.
	 * If the absolute value of the new value minus the old value for all
	 * nodes is less than the variance, then the network has converged.
	 */
	public static double calculateVariance(int sizeOfNetwork) {
		double initialValue = (double)1/sizeOfNetwork;
		double variance = initialValue * Math.pow(10, -5);
		return variance;
	}
	
	/*
	 * This method checks for and fixes network sinks. A sink is a node
	 * that has no out edges, this trapping pagerank values. A sink is
	 * fixed by adding outward edges to all other nodes.
	 */
	public static void fixNetworkSinks(List<Node> network) {
		for (Node n : network) {
			if (n.getEdges().isEmpty()) for (Node e : network) n.addEdge(e);
		}
	}
	
	/*
	 * This method initializes all node values to 1 divided by the size of the network.
	 */
	public static void initializeValues(List<Node> network) {
		for (Node node : network) {
			node.setFutureValue(1/(double) network.size());
		}
	}
	
	/*
	 * This method runs the iterative version of the pagerank algorithm with a damping
	 * factor of .85. It then returns the network with the appropriate pagerank values.
	 */
	public static List<Node> pageRankIterativeScaled(List<Node> network, double variance) {
		
		// This is the damping factor for the network
		double d = .85;
		
		// Run generate the new values for each node until convergence
		while (checkConverge(network, variance)) {
			// For each node set the value to the future value and reset the future value
			stepValues(network);
			for (Node node : network) {
				// Distribute 1 minus the damping factor to each node
				node.updateFutureValue((1-d)/network.size());
				// Calculate the increase of distributing the node's value 
				// to each node it points to, times the damping factor
				double increase = d*(node.getValue()/node.getEdges().size());
				// Distribute the increase to each connected node
				for (Node n : node.getEdges()) {
					n.updateFutureValue(increase);
				}
			} 
		}
		
		// Do one final step and normalize all values over the variance
		stepAndNormalizeValues(network, variance);
		
		return network;
	}
	
	/*
	 * This method steps the future value for each node to the actual value
	 * and then resets the future value to 0.
	 */
	public static void stepValues(List<Node> network) {
		for (Node node : network) {
			node.setValue(node.getFutureValue());
			node.setFutureValue(0);
		}
	}
	
	/*
	 * This performs one final step and normalizes the values by the variance.
	 */
	public static void stepAndNormalizeValues(List<Node> network, double variance) {
		for (Node node : network) {
			node.setValue(Math.ceil(node.getFutureValue()/variance)*variance);
			node.setFutureValue(0);
		}
	}
	
	/*
	 * This method checks for convergence in the network. If all of the node's value minus 
	 * its future value is less than the variance, then the network has converged.
	 */
	public static boolean checkConverge(List<Node> network, double variance) {
		boolean check = false;
		for (Node node : network) {
			if (Math.abs(node.getValue() - node.getFutureValue()) > variance) {
				check = true;
				break;
			}
		} return check;
	}
	
	/*
	 * This method runs the matrix version of the pagerank algorithm with a damping
	 * factor of .85. It then returns the network with the appropriate pagerank values.
	 */
	public static List<Node> pageRankRandomWalkScaled(List<Node> network, double variance) {
		
		// The damping factor is set to .85
		double d = .85;
		
		// An n x n matrix is created which will be used to update the future values
		double[][] N = toNMatrixScaled(network, d);
		// The matrix is transposed so it can be multiplied by the n x 1 b matrix
		double[][] NT = transposeMatrix(N);
		// A matrix to store the values for the nodes, initialized to 1 / the size of the network
		double[][] b = toBMatrix(network);
		// A matrix to store the prior iteration's values
		double[][] bP = new double[b.length][1];
		
		// Update the values until convergence
		while (checkConverge(b, bP, variance)) {
			// Set the bP matrix to the old b matrix
			bP = b;
			// Multiply the NT matrix by the b matrix to calculate the future values
			b = multiplyMatrices(NT, b);
			// Check for a leak of value in the network
			double leak = 1/getColumnTotals(b)[0];
			// Redistribute the leak back to the network
			b = constantMultiply(b, leak);
		}
		
		// After convergence set the values of the nodes to the matrix values
		for (int i = 0; i < network.size(); i++) network.get(i).setValue(Math.round(b[i][0]/variance)*variance);
		
		return network;
	}
	
	/*
	 * This method creates the N matrix, which is an n x n matrix that stores the relation
	 * values of directed edges. It is also scaled by the given damping factor d. 
	 */
	public static double[][] toNMatrixScaled(List<Node> network, double d) {
		
		int length = network.size();
		double[][] matrix = new double[length][length];
		
		for (Node node: network) {
			for (Node n: node.getEdges()) {
				// If a node has an edge to another node, the position in the matrix
				// is set to the value of d / the number of edges out of the from node
				matrix[network.indexOf(node)][network.indexOf(n)] = d*1/node.getEdges().size();
			}
		}
		
		// Redistribute the remaining 1 - d to each row in the matrix
		redistribute(matrix, 1-d, matrix.length);
		
		return matrix;
	}
	
	/*
	 * This method redistributes a given value / divisor to all matrix indices.
	 */
	public static void redistribute(double[][] matrix, double value, int divisor) {
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[0].length; j++) {
				matrix[i][j] += value/divisor;
			}
		}
	}
	
	/*
	 * This method creates the b matrix, an n x 1 matrix with all values set to 1 / n.
	 */
	public static double[][] toBMatrix(List<Node> network) {
		
		int length = network.size();
		double[][] matrix = new double[length][1];
		
		for (int i = 0; i < length; i++) {
			matrix[i][0] = (double)1/length;
		}
		
		return matrix;
	}
	
	/*
	 * This method checks for the convergence of the network by comparing the values of the
	 * prior and current values of the b matrices and checking if they are less than the variance.
	 */
	public static boolean checkConverge(double[][] prior, double[][] after, double variance) {
		boolean check = false;
		if (prior.length != after.length) return false;
		for (int i = 0; i < prior.length; i++) {
			for (int j = 0; j < prior[0].length; j++) {
				if (Math.abs(prior[i][j] - after[i][j]) > variance) {
					check = true;
					break;
				}
			}
		} return check;
	}
	
	/*
	 * This matrix takes two matrices and returns the matrix multiplication of them.
	 */
	public static double[][] multiplyMatrices(double[][] matrix1, double[][] matrix2) {
		if (matrix1[0].length != matrix2.length) return null;
		double[][] result = new double[matrix1.length][matrix2[0].length];
		for (int i = 0; i < matrix1.length; i++) {
			for (int j = 0; j < matrix2[0].length; j++) {
				for (int k = 0; k < matrix1[0].length; k++) {
					result[i][j] += matrix1[i][k] * matrix2[k][j];
				}
			}
		} return result;
	}
	
	/*
	 * This method takes in a matrix and returns its transpose.
	 */
	public static double[][] transposeMatrix(double[][] matrix) {
		double[][] result = new double[matrix[0].length][matrix.length];
		for (int i = 0; i < result.length; i++) {
			for (int j = 0; j < result[0].length; j++) {
				result[i][j] = matrix[j][i];
			}
		}return result;
	}
	
	/*
	 * This method takes in a matrix and returns an array with the sum
	 * of each column in the matrix.
	 */
	public static double[] getColumnTotals(double[][] matrix) {
		double[] totals = new double[matrix[0].length];
    	for (int i = 0; i < matrix[0].length; i++) {
			double acc = 0;
			for (int j = 0; j < matrix.length; j++) {
				acc += matrix[j][i];
			} totals[i] = acc;
		} return totals;
    }
	
	/*
	 * This method takes in a constant and a matrix and multiplies 
	 * every value in the matrix by the constant.
	 */
	public static double[][] constantMultiply(double[][] matrix, double c) {
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[0].length; j++) {
				matrix[i][j] = c*matrix[i][j];
			}
		} return matrix;
	}
	
	/*
	 * This methods takes two matrices and returns the addition of them.
	 */
	public static double[][] addMatrices(double[][] matrix1, double[][] matrix2) {
		double[][] matrix = new double[matrix1.length][matrix1[0].length];
		if (matrix1.length == matrix2.length && matrix1[0].length == matrix2[0].length) {
			for (int i = 0; i < matrix1.length; i++) {
				for (int j = 0; j < matrix1[0].length; j++) {
					matrix[i][j] = matrix1[i][j] + matrix2[i][j];
				}
			}
		} return matrix;
	}
	
	/*
	 * This method uses a traditional merge sort on the network to return the network
	 * when it is sorted by its converged pagerank values.
	 */
	public static List<Node> sort(List<Node> network) {
		List<Node> orderedList = mergeSort(network);
    	return orderedList;
    }
    
    public static List<Node> mergeSort(List<Node> network) {
    	if (network.size() <= 1) {
    		return network;
    	} 
    	
    	List<Node> left = new ArrayList<Node>();
    	left.addAll(network.subList(0, network.size()/2));
    	List<Node> right = new ArrayList<Node>();
    	right.addAll(network.subList(network.size()/2, network.size()));
    	
    	mergeSort(left);
    	mergeSort(right);
    	
    	merge(left, right, network);
    	return network;
    }
    
    public static void merge(List<Node> left, List<Node> right, List<Node> network) {
    	int l1 = 0;
    	int r1 = 0;
    	int current = 0;
    	while((l1 < left.size()) && (r1 < right.size())) {
    		if (left.get(l1).getValue() > right.get(r1).getValue()) {
    			network.set(current, left.get(l1));
    			l1++;
    		} else if (left.get(l1).getValue() == right.get(r1).getValue()) {
    			if (left.get(l1).getName().compareTo(right.get(r1).getName()) < 0) {
    				network.set(current, left.get(l1));
        			l1++;
    			} else {
    				network.set(current, right.get(r1));
        			r1++;
    			}
    		} else {
    			network.set(current, right.get(r1));
    			r1++;
    		} current++;
    	}
    	for (int i = r1; i < right.size(); i++) {
    		network.set(current++, right.get(i));
    	} for (int i = l1; i < left.size(); i++) {
    		network.set(current++, left.get(i));
    	}
    }
    
    /*
     * This method prints the a given list.
     */
    public static void printList(List<Node> list) {
    	for (Node n : list) n.print();
    }
    
    /*
     * This method prints a given matrix.
     */
    public static void printMatrix(double[][] matrix) {
    	for (int i = 0; i < matrix.length; i++) {
    		for (int j = 0; j < matrix[0].length; j++) {
    			System.out.printf("%f ", matrix[i][j]);
    		} System.out.println();
    	}
    }
    
    /*
     * This method prints a given matrix vector (i.e. n x 1).
     */
    public static void printMatrixVector(double[][] matrix) {
    	if (matrix[0].length != 1) return;
    	for (int i = 0; i < matrix.length; i++) {
    		System.out.printf("%f ", matrix[i][0]);
    	} System.out.println();
    }
}