package org.dataalgorithms.chap11.statemodel;

import java.util.Map;
import java.util.List;
import java.util.HashMap;

/**
 * Markov state transition probability matrix builder
 *
 */
public class StateTransitionTableBuilder {

	//
	// model.states=SL,SE,SG,ML,ME,MG,LL,LE,LG
	//
	// states<key, value>: key is the state and value is row/column in table
	//
	private Map<String, Integer> states = null;
	private double[][] table = null;
    private int numberOfStates;
	private int scale = 100;

	private void initStates(){
	    states = new HashMap<String, Integer>();
		states.put("SL", 0);	
		states.put("SE", 1);	
		states.put("SG", 2);	
		states.put("ML", 3);	
		states.put("ME", 4);	
		states.put("MG", 5);	
		states.put("LL", 6);	
		states.put("LE", 7);	
		states.put("LG", 8);	
	}
			
	public StateTransitionTableBuilder(int numberOfStates) {
		this.numberOfStates = numberOfStates;
		table = new double[numberOfStates][numberOfStates];
		initStates();
	}
	
	public StateTransitionTableBuilder(int numberOfStates, int scale) {
		this(numberOfStates);
		this.scale = scale;
	}

    public void add(String fromState, String toState, int count) {
    	int row = states.get(fromState);
    	int column = states.get(toState);
        table[row][column] = count;
    }
    
	public void normalizeRows() {
		// Laplace correction: the usual solution is to do a 
		// Laplacian correction by upping all the counts by 1
		// see: http://cs.nyu.edu/faculty/davise/ai/bayesText.html		
		for (int r = 0; r < numberOfStates; r++) {
			boolean gotZeroCount = false;
			for (int c = 0; c < numberOfStates; c++) {
				if(table[r][c] == 0) {
					gotZeroCount = true;
					break;
				}
			}
			
			if (gotZeroCount) {
				for (int c = 0; c < numberOfStates; c++) {
					 table[r][c] += 1;
				}			
			}
		}		
		
		//normalize
		for (int r = 0; r < numberOfStates; r++) {
			double rowSum = getRowSum(r);
			for (int c = 0; c < numberOfStates; c++) {
				table[r][c] = table[r][c] / rowSum;
			}
		}
	}
	
    public double getRowSum(int rowNumber) {
        double sum = 0.0;
        for (int column = 0; column < numberOfStates; column++) {
            sum += table[rowNumber][column];
        }
        return sum;
    }

    public String serializeRow(int rowNumber) {
        StringBuilder builder = new StringBuilder();
        for (int column = 0; column < numberOfStates; column++) {
        	double element = table[rowNumber][column];
        	builder.append(String.format("%.4g", element));
            if (column < (numberOfStates-1)) {
            	builder.append(",");
            }
        }
        return builder.toString();
    }

    public void persistTable() {
		for (int row = 0; row < numberOfStates; row++) {
        	String serializedRow = serializeRow(row);
        	System.out.println(serializedRow);
        }
    }
   
	public static void generateStateTransitionTable(String hdfsDirectory) {
		List<TableItem> list = ReadDataFromHDFS.readDirectory(hdfsDirectory);
	    StateTransitionTableBuilder tableBuilder = new StateTransitionTableBuilder(9);
	    for (TableItem item : list) {
	    	tableBuilder.add(item.fromState, item.toState, item.count);
	    }
	    
	    tableBuilder.normalizeRows();
	    tableBuilder.persistTable();
	}
	
	public static void main(String[] args) {
		String hdfsDirectory = args[0];
		generateStateTransitionTable(hdfsDirectory);
	}	
}
