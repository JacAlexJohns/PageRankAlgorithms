package pageRankAlgorithm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class PartitionPageRank {
	
	/*
	 * Input: Any number of filepaths linking to .csv files
	 * Output: The runtime for each of the parallelized (multithreaded) pagerank algorithm
	 * This also prints the results for the parallelized algorithm to .csv files
	 * The following program runs the parallelized iterative pagerank algorithm
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
				runParallelized(fileName);
			}
		} sc.close();
		
	}
	
	/*
	 * This method runs the parallelized (multithreaded) iterative page rank algorithm 
	 * on a given file. It also prints out the running time of the algorithm in milliseconds.
	 */
	public static List<AtomicNode> runParallelized(String fileName) throws Exception {
		// Build the network on which the pagerank algorithm will be run
		List<AtomicNode> network = buildNetwork(fileName);
		// Calculate the variance which will determine when the algorithm converges
		double variance = PageRank.calculateVariance(network.size());
		
		System.out.println("Parallelized:");
		final long startTime = System.currentTimeMillis();
		// Run the pagerank algorithm for the network using the calculated variance
		network = pageRankParallelized(network, variance, 4);
		final long endTime = System.currentTimeMillis();
		System.out.println("Run Time: " + ((double)endTime - startTime) + " ms");
		
		return network;
	}
	
	/*
	 * This method takes in a filename and builds the network that will be used
	 * in the pagerank algorithm. (NOTE: it is assumed that the file is a .csv
	 * file and that it is undirected. A directed network can still be created
	 * if the .csv file has an edge going in both directions towards every pair
	 * of connected atomic nodes.)
	 */
	@SuppressWarnings("resource")
	public static List<AtomicNode> buildNetwork(String fileName) throws Exception {
		// A BufferedReader to read in the .csv file
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		
		String line;
		List<AtomicNode> network = new ArrayList<AtomicNode>();
		
		// For each line in the .csv file, build an edge from the first atomic node to the second
		while ((line = br.readLine()) != null) {
			String[] lineItems = line.split(",");
			// Create the from and to atomic nodes
			AtomicNode AtomicNodeFrom = new AtomicNode(lineItems[0]);
			AtomicNode AtomicNodeTo = new AtomicNode(lineItems[2]);
			// Check if the network already contains either atomic nodes
			if (network.contains(AtomicNodeFrom)) AtomicNodeFrom = network.get(network.indexOf(AtomicNodeFrom));
			else network.add(AtomicNodeFrom);
			if (network.contains(AtomicNodeTo)) AtomicNodeTo = network.get(network.indexOf(AtomicNodeTo));
			else network.add(AtomicNodeTo);
			// Create an edge from the AtomicNodeFrom to the AtomicNodeTo
			AtomicNodeFrom.addEdge(AtomicNodeTo);
		}
		
		// Fix all network sinks
		fixNetworkSinks(network);
		// Initialize the values of the network
		initializeValues(network);
		
		return network;
	}
	
	/*
	 * This method checks for and fixes network sinks. A sink is an atomic node
	 * that has no out edges, this trapping pagerank values. A sink is
	 * fixed by adding outward edges to all other atomic nodes.
	 */
	public static void fixNetworkSinks(List<AtomicNode> network) {
		for (AtomicNode n : network) {
			if (n.getAtomicEdges().isEmpty()) for (AtomicNode e : network) n.addEdge(e);
		}
	}
	
	/*
	 * This method initializes all atomic node values to 1 divided by the size of the network.
	 */
	public static void initializeValues(List<AtomicNode> network) {
		for (AtomicNode AtomicNode : network) {
			AtomicNode.setAtomicFValue(1/(double) network.size());
		}
	}
	
	/*
	 * This method runs the actual parallelized pagerank algorithm. It takes in a network,
	 * the variance to check for convergence, and the number of threads that should be spawned
	 * to split the workload into.
	 */
	public static List<AtomicNode> pageRankParallelized(List<AtomicNode> network, double variance, int threads) {
		
		// Set the damping factor to .85
		double damping = .85;
		
		// A list of partitions which the network will be broken into, where one thread runs on each
		List<List<AtomicNode>> partitions = new ArrayList<List<AtomicNode>>();
		// The partition size given the number of threads
		int pSize = (int) Math.floor((double)network.size()/threads);
		// Breaks the network into t partitions, where t is the number of threads
		for (int i = 0; i < threads; i++) {
			int startPos = i * pSize;
			int endPos = i == (threads - 1) ? network.size() : (i + 1) * pSize;
			partitions.add(i, network.subList(startPos, endPos));
		}
		
		// Continually iterate running the threads on the partitions until convergence
		while (checkConverge(network, variance)) {
			startPartitionThreads(network, partitions, damping, threads);
		}
		
		// Set and normalize the final atomic node values
		stepAndNormalizeValues(network, variance);
		
		return network;
	}
	
	/*
	 * This method starts the threads on the given partitions and waits for them to finish.
	 */
	public static void startPartitionThreads(List<AtomicNode> network, List<List<AtomicNode>> partitions, 
			double damping, int threads) {
		
		// The damping amount which each node will gain at the beginning of each iteration
		double dampingAmount = (1 - damping) / network.size();
		
		// A CountDownLatch to keep track of when the threads all finish
		final CountDownLatch latch = new CountDownLatch(threads);
		
		// Set the atomic value to the future atomic value and reset the future atomic value to 0
		stepValues(network);
		
		// Create a thread for each partition and run it on the partition
		for (int i = 0; i < threads; i ++) {
			new PartitionThread(partitions.get(i), latch, damping, dampingAmount).start();
		}
		
		// Wait for all of the threads to finish
		try {
			latch.await();
		} catch (InterruptedException ex) {
			throw new RuntimeException("Errors Galore!!!");
		}
	}
	
	/*
	 * This method steps the future value for each atomic node to the actual value
	 * and then resets the future value to 0.
	 */
	public static void stepValues(List<AtomicNode> network) {
		for (AtomicNode atomicNode : network) {
			atomicNode.setValue(atomicNode.getAtomicFValue());
			atomicNode.setAtomicFValue(0);
		}
	}
	
	/*
	 * This performs one final step and normalizes the values by the variance.
	 */
	public static void stepAndNormalizeValues(List<AtomicNode> network, double variance) {
		for (AtomicNode node : network) {
			node.setValue(Math.ceil(node.getAtomicFValue()/variance)*variance);
			node.setAtomicFValue(0);
		}
	}
	
	/*
	 * This method checks for convergence in the network. If all of the atomic node's value minus 
	 * its future value is less than the variance, then the network has converged.
	 */
	public static boolean checkConverge(List<AtomicNode> network, double variance) {
		boolean check = false;
		for (AtomicNode AtomicNode : network) {
			if (Math.abs(AtomicNode.getValue() - AtomicNode.getAtomicFValue()) > variance) {
				check = true;
				break;
			}
		} return check;
	}
	
	/*
	 * This method uses a traditional merge sort on the network to return the network
	 * when it is sorted by its converged pagerank values.
	 */
	public static List<AtomicNode> sort(List<AtomicNode> network) {
		List<AtomicNode> orderedList = mergeSort(network);
    	return orderedList;
    }
    
    public static List<AtomicNode> mergeSort(List<AtomicNode> network) {
    	if (network.size() <= 1) {
    		return network;
    	} 
    	
    	List<AtomicNode> left = new ArrayList<AtomicNode>();
    	left.addAll(network.subList(0, network.size()/2));
    	List<AtomicNode> right = new ArrayList<AtomicNode>();
    	right.addAll(network.subList(network.size()/2, network.size()));
    	
    	mergeSort(left);
    	mergeSort(right);
    	
    	merge(left, right, network);
    	return network;
    }
    
    public static void merge(List<AtomicNode> left, List<AtomicNode> right, List<AtomicNode> network) {
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
}
