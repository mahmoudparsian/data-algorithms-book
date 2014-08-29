package org.dataalgorithms.chap11.statemodel;

/**
 * TableItem represents an item of a Markov State Transition Model 
 * as a Tuple3<fromSate, toState, count>
 *
 */
public class TableItem  {
	String fromState;
	String toState;
	int count;
	
	public TableItem(String fromState, String toState, int count) {
		this.fromState = fromState;
		this.toState = toState;
		this.count = count;
	}
	
	/**
	 * for debugging ONLY
	 */
	public String toString() {
		return "{"+fromState+"," +toState+","+count+"}";
	}
}
