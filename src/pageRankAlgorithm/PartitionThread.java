package pageRankAlgorithm;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class PartitionThread extends Thread {
	
	private List<AtomicNode> partition;
	private CountDownLatch latch;
	private double damping;
	private double dampingAmount;
	
	/*
	 * This is a thread that distributes pagerank on only a partition of the total network.
	 * It still accesses atomic nodes outside of the partition through edges, and therefore the
	 * atomicity of the nodes is essential to ensure singular access to their data.
	 */
	public PartitionThread(List<AtomicNode> partition, CountDownLatch latch, double damping, double dampingAmount) {
		this.partition = partition;
		this.latch = latch;
		this.damping = damping;
		this.dampingAmount = dampingAmount;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Thread#run()
	 * This method overrides the original thread run() method.
	 * This method distributes pagerank values throughout the partitioned
	 * network. It then counts down the CountDownLatch when done.
	 */
	@Override
	public void run() {
		// For each atomic node in the partition
		for (AtomicNode n : partition) {
			// Increase the atomic future value by the damping amount (1-d)/n
			n.getAndAddFValue(dampingAmount);
			int numEdges = n.getAtomicEdges().size();
			// Calculate the increase to be distributed to n's outward edges
			double increase = damping * n.getValue() / numEdges;
			for (AtomicNode e : n.getAtomicEdges()) {
				// Update each outward atomic node e by the increase
				e.getAndAddFValue(increase);
			}
		} latch.countDown(); // Count down the latch to signal the termination of the thread
	}

}
