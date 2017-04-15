package pageRankAlgorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class AtomicNode extends Node {
	
	private AtomicReference<Double> atomicValue;
	private AtomicReference<Double> atomicFValue;
	private List<AtomicNode> atomicEdges;
	
	/*
	 * The AtomicNode class takes in a name, which will be the name of the node.
	 * It also contains an atomic value, atomic future value, and a list of atomic nodes
	 * which it has outgoing edges to. The atomic nature of the node means that it
	 * will accurately update the values in a parallelized algorithm.
	 */
	public AtomicNode(String name) {
		super(name);
		this.atomicValue = new AtomicReference<Double>(Double.valueOf(0.0));
		this.atomicFValue = new AtomicReference<Double>(Double.valueOf(0.0));
		this.atomicEdges = new ArrayList<AtomicNode>();
	}
	
	/*
	 * Gets the atomic value.
	 */
	public double getAtomicValue() {
        return atomicValue.get();
    }
	
	/*
	 * Sets the atomic value.
	 */
	public void setAtomicValue(double newV) {
		while (true) {
            Double currentValue = atomicValue.get();
            if (atomicValue.compareAndSet(currentValue, newV))
                break;
        }
    }
	
	/*
	 * Increases the atomic value by the delta value and returns the new atomic value.
	 */
	public double getAndAddValue(double delta) {
        while (true) {
            Double currentValue = atomicValue.get();
            Double newValue = Double.valueOf(currentValue.doubleValue() + delta);
            if (atomicValue.compareAndSet(currentValue, newValue))
                return currentValue.doubleValue();
        }
    }
	
	/*
	 * Gets the atomic future value.
	 */
	public double getAtomicFValue() {
        return atomicFValue.get();
    }
	
	/*
	 * Sets the atomic future value.
	 */
	public void setAtomicFValue(double newV) {
		while (true) {
            Double currentValue = atomicFValue.get();
            if (atomicFValue.compareAndSet(currentValue, newV))
                break;
        }
    }
	
	/*
	 * Increases the atomic future value by the delta value and returns the new atomic future value.
	 */
	public double getAndAddFValue(double delta) {
        while (true) {
            Double currentValue = atomicFValue.get();
            Double newValue = Double.valueOf(currentValue.doubleValue() + delta);
            if (atomicFValue.compareAndSet(currentValue,  newValue))
            	return currentValue.doubleValue();
        }
    }
	
	/*
	 * Gets the outgoing atomic edges.
	 */
	public List<AtomicNode> getAtomicEdges() {
		return this.atomicEdges;
	}
	
	/*
	 * Adds an outgoing atomic edge.
	 */
	public void addEdge(AtomicNode edge) {
		this.atomicEdges.add(edge);
	}
	
	// Print certain aspects of the node
	public void print() {
		List<AtomicNode> edges = this.atomicEdges;
		System.out.println(this.getName() + ": " + edges.size() + " edges");
		System.out.println("Value: " + this.getName());
	}
	
	/*
	 * Equality and hash code only checked for name
	 */
	public boolean equals(Object that) {
		if (this == that) return true;
		if (!(this instanceof AtomicNode)) return false;
		AtomicNode aThat = (AtomicNode) that;
		return this.getName().equals(aThat.getName());
	}
	public int hashCode() {
		return this.getName().hashCode();
	}

}
