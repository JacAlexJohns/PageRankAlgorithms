package pageRankAlgorithm;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Scanner;

public class PageRankBigDecimal {
	
	/*
	 * Input: Any number of filepaths linking to .csv files
	 * Output: The runtime for the BigDecimal matrix pagerank algorithm
	 * This also prints the results for the three algorithms to .csv files
	 * The following program runs the BigDecimal matrix pagerank algorithm
	 * on given .csv files.
	 */
	public static void main(String[] args) throws Exception {
		
		System.out.println("Input Filenames: ");
		// Read in input file names
		Scanner sc = new Scanner(System.in);
		while (sc.hasNextLine()) {
			// Only valid file names will be accepted, otherwise an exception will be thrown
			String fileName = sc.nextLine().trim();
			System.out.println("\n" + fileName);
			if (!fileName.equals("")) {
				// Run the pagerank algorithm on the files
				runMatrix(fileName);
			}
		} sc.close();
		
	}
	
	/*
	 * This method runs the BigDecimal matrix page rank algorithm on a given file.
	 * It also prints out the running time of the algorithm in milliseconds.
	 */
	public static List<Node> runMatrix(String fileName) throws Exception {
		// Build the network on which the pagerank algorithm will be run
		List<Node> network = PageRank.buildNetwork(fileName);
		// Calculate the variance which will determine when the algorithm converges
		double variance = PageRank.calculateVariance(network.size());
		
		System.out.println("Random Walk:");
		final long startTime = System.currentTimeMillis();
		// Run the pagerank algorithm for the network using the calculated variance
		network = pageRankRandomWalkScaled(network, variance);
		final long endTime = System.currentTimeMillis();
		System.out.println("Run Time: " + ((double)endTime - startTime) + " ms");
		
		return network;
	}
	
	/*
	 * This method runs the BigDecimal matrix version of the pagerank algorithm with a damping
	 * factor of .85. It then returns the network with the appropriate pagerank values.
	 */
	public static List<Node> pageRankRandomWalkScaled(List<Node> network, double variance) {
		
		// The damping factor is set to .85
		double d = .85;
		
		// An n x n matrix is created which will be used to update the future values
		BigDecimal[][] N = toNMatrixScaled(network, d);
		// The matrix is transposed so it can be multiplied by the n x 1 b matrix
		BigDecimal[][] NT = transposeMatrix(N);
		// A matrix to store the values for the nodes, initialized to 1 / the size of the network
		BigDecimal[][] b = toBMatrix(network);
		// A matrix to store the prior iteration's values
		BigDecimal[][] bP = toEmptyMatrix(new BigDecimal[b.length][1]);
		
		// Update the values until convergence
		while (checkConverge(b, bP, variance)) {
			// Set the bP matrix to the old b matrix
			bP = b;
			// Multiply the NT matrix by the b matrix to calculate the future values
			b = multiplyMatrices(NT, b);
			// Check for a leak of value in the network
			BigDecimal leak = getColumnTotals(b)[0];
			// Redistribute the leak back to the network
			b = constantDivide(b, leak);
		}
		
		// After convergence set the values of the nodes to the matrix values and normalize
		for (int i = 0; i < network.size(); i++) network.get(i).setValue(Math.ceil(b[i][0].doubleValue()/variance)*variance);
		
		return network;
	}
	
	/*
	 * This method creates the N matrix, which is an n x n matrix that stores the relation
	 * values of directed edges. It is also scaled by the given damping factor d. 
	 */
	public static BigDecimal[][] toNMatrixScaled(List<Node> network, double d) {
		
		int length = network.size();
		BigDecimal[][] matrix = new BigDecimal[length][length];
		
		for (Node node: network) {
			for (Node n: node.getEdges()) {
				// If a node has an edge to another node, the position in the matrix
				// is set to the value of d / the number of edges out of the from node
				matrix[network.indexOf(node)][network.indexOf(n)] = new BigDecimal(d*1/node.getEdges().size());
			}
		}
		
		// Redistribute the remaining 1 - d to each row in the matrix
		redistribute(matrix, new BigDecimal(1-d), length);
		
		return matrix;
	}
	
	/*
	 * This method redistributes a given value / divisor to all matrix indices.
	 */
	public static void redistribute(BigDecimal[][] matrix, BigDecimal value, int length) {
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[0].length; j++) {
				if (matrix[i][j] == null) matrix[i][j] = BigDecimal.ZERO;
				matrix[i][j] = matrix[i][j].add(value.divide(new BigDecimal(length), RoundingMode.HALF_EVEN));
			}
		}
	}
	
	/*
	 * This method creates the b matrix, an n x 1 matrix with all values set to 1 / n.
	 */
	public static BigDecimal[][] toBMatrix(List<Node> network) {
		
		int length = network.size();
		BigDecimal[][] matrix = new BigDecimal[length][1];
		
		for (int i = 0; i < length; i++) {
			matrix[i][0] = new BigDecimal((double)1/length);
		}
		
		return matrix;
	}
	
	/*
	 * This method creates an empty n x 1 matrix, since a BigDecimal array is not initialized to all zeros.
	 */
	public static BigDecimal[][] toEmptyMatrix(BigDecimal[][] matrix) {
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[0].length; j++) {
				matrix[i][j] = BigDecimal.ZERO;
			}
		} return matrix;
	}
	
	/*
	 * This method checks for the convergence of the network by comparing the values of the
	 * prior and current values of the b matrices and checking if they are less than the variance.
	 */
	public static boolean checkConverge(BigDecimal[][] prior, BigDecimal[][] after, double variance) {
		boolean check = false;
		if (prior.length != after.length) return false;
		for (int i = 0; i < prior.length; i++) {
			for (int j = 0; j < prior[0].length; j++) {
				if (prior[i][j].subtract(after[i][j]).abs().doubleValue() > variance) {
					check = true;
					break;
				}
			}
		} return check;
	}
	
	/*
	 * This method takes in a constant and a matrix and multiplies 
	 * every value in the matrix by the constant.
	 */
	public static BigDecimal[][] constantMultiply(BigDecimal[][] matrix, BigDecimal c) {
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[0].length; j++) {
				matrix[i][j] = matrix[i][j].multiply(c);
			}
		} return matrix;
	}
	
	/*
	 * This method takes in a constant and a matrix and divides 
	 * every value in the matrix by the constant.
	 */
	public static BigDecimal[][] constantDivide(BigDecimal[][] matrix, BigDecimal c) {
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[0].length; j++) {
				matrix[i][j] = matrix[i][j].divide(c, RoundingMode.HALF_EVEN);
			}
		} return matrix;
	}
	
	/*
	 * This matrix takes two matrices and returns the matrix multiplication of them.
	 */
	public static BigDecimal[][] multiplyMatrices(BigDecimal[][] matrix1, BigDecimal[][] matrix2) {
		if (matrix1[0].length != matrix2.length) return null;
		BigDecimal[][] result = new BigDecimal[matrix1.length][matrix2[0].length];
		for (int i = 0; i < result.length; i++) {
			for (int j = 0; j < result[0].length; j++) {
				result[i][j] = BigDecimal.ZERO;
				for (int k = 0; k < matrix1[0].length; k++) {
					if (result[i][j] == null) {
						result[i][j] = BigDecimal.ZERO;
					} result[i][j] = result[i][j].add(matrix1[i][k].multiply(matrix2[k][j]));
				}
			}
		} return result;
	}
	
	/*
	 * This method takes in a matrix and returns its transpose.
	 */
	public static BigDecimal[][] transposeMatrix(BigDecimal[][] matrix) {
		BigDecimal[][] result = new BigDecimal[matrix[0].length][matrix.length];
		for (int i = 0; i < result.length; i++) {
			for (int j = 0; j < result[0].length; j++) {
				result[i][j] = matrix[j][i];
			}
		} return result;
	}
	
	/*
	 * This method takes in a matrix and returns an array with the sum
	 * of each column in the matrix.
	 */
	public static BigDecimal[] getColumnTotals(BigDecimal[][] matrix) {
		BigDecimal[] totals = new BigDecimal[matrix[0].length];
    	for (int i = 0; i < matrix[0].length; i++) {
			BigDecimal acc = BigDecimal.ZERO;
			for (int j = 0; j < matrix.length; j++) {
				acc = acc.add(matrix[j][i]);
			} totals[i] = acc;
		} return totals;
    }
    
	/*
     * This method prints a given matrix.
     */
    public static void printMatrix(BigDecimal[][] matrix) {
    	for (int i = 0; i < matrix.length; i++) {
    		for (int j = 0; j < matrix[0].length; j++) {
    			System.out.printf("%f ", matrix[i][j]);
    		} System.out.println();
    	}
    }
    
    /*
     * This method prints a given matrix vector (i.e. n x 1).
     */
    public static void printMatrixVector(BigDecimal[][] matrix) {
    	for (int i = 0; i < matrix.length; i++) {
    		System.out.printf("%f ", matrix[i][0]);
    	} System.out.println();
    }
}
