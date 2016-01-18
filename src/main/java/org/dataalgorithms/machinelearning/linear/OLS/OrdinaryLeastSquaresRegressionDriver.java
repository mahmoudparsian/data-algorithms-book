package org.dataalgorithms.machinelearning.linear.OLS;

import java.io.FileReader;
import java.io.BufferedReader;
import org.apache.commons.lang.StringUtils;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

/**
 * This class is a driver class for the POJO solution using 
 * the OLSMultipleLinearRegression class.
 * 
 * The goal is to use the built linear regression model and 
 * predict the price of a car.
 *     
 * 
 * 
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class OrdinaryLeastSquaresRegressionDriver {	
	   
    
    public static void main(String[] args) throws Exception {
        //
        // read input parameters
        //
        int TRAINING_SIZE = Integer.parseInt(args[0]);
        String trainingFilename = args[1];
        String queryFilename = args[2];
          
        //
        // use training data set file and build regression model
        //
        OLSMultipleLinearRegression regression = 
            OrdinaryLeastSquaresRegressionModel.buildModel(trainingFilename, TRAINING_SIZE);   
        
        //
        // Predict by using query data set and the built regression model 
        //
        BufferedReader br = new BufferedReader(new FileReader(queryFilename));
        try {
            //// prepare X 
            String queryRecord;
            while ( (queryRecord = br.readLine()) != null) {
                // record: <Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
                String[] tokens = StringUtils.split(queryRecord, ",");
                double[] features = new double[tokens.length];
                for (int i = 0; i < features.length; i++) {
                    features[i] = Double.parseDouble(tokens[i]);
                } 
                double price = OrdinaryLeastSquaresRegressionModel.predict(regression, features);
                System.out.println("queryRecord="+queryRecord);
                System.out.println("predicted price="+price);
            }
        } 
        finally {
            br.close();
        }
    }
	  	
}
