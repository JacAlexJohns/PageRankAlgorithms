package pageRankAlgorithm;

import java.util.ArrayList;
import java.util.List;

public class Node {
	
	private String name;
	private double value;
	private double futureValue;
	private List<Node> edges;
	
	/*
	 * The Node class takes in a name, which will be the name of the node.
	 * It also contains a current value, future value, and a list of nodes
	 * which it has outgoing edges to.
	 */
	public Node(String name) {
		this.name = name;
		this.value = 0;
		this.futureValue = 0;
		this.edges = new ArrayList<Node>();
	}
	
	/*
	 * Gets the name.
	 */
	public String getName() {
		return this.name;
	}
	
	/*
	 * Gets the value.
	 */
	public double getValue() {
		return this.value;
	}
	
	/*
	 * Sets the value.
	 */
	public void setValue(double value) {
		this.value = value;
	}
	
	/*
	 * Gets the future value.
	 */
	public double getFutureValue() {
		return this.futureValue;
	}
	
	/*
	 * Sets the future value.
	 */
	public void setFutureValue(double value) {
		this.futureValue = value;
	}
	
	/*
	 * Increases the future value by the given value.
	 */
	public void updateFutureValue(double value) {
		this.futureValue += value;
	}
	
	/*
	 * Gets the outgoing edges.
	 */
	public List<Node> getEdges() {
		return this.edges;
	}
	
	/*
	 * Adds an outgoing edge.
	 */
	public void addEdge(Node edge) {
		this.edges.add(edge);
	}
	
	// Print certain aspects of the node
	public void print() {
		List<Node> edges = this.edges;
		System.out.println(this.name + ": " + edges.size() + " edges");
		System.out.println("Value: " + this.value);
	}
	
	/*
	 * Equality and hash code only checked for name
	 */
	public boolean equals(Object that) {
		if (this == that) return true;
		if (!(this instanceof Node)) return false;
		Node aThat = (Node) that;
		return this.name.equals(aThat.name);
	}
	public int hashCode() {
		return this.name.hashCode();
	}

}
