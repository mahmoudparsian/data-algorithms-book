package org.dataalgorithms.machinelearning.linear.OLS;

import java.io.FileReader;
import java.io.BufferedReader;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

/**
 * Implements ordinary least squares (OLS) to estimate the 
 * parameters of a multiple linear regression model.
 * 
 * Training data set:
 $ wc  -l  ToyotaCorolla_Transformed_without_head.csv
 1436 ToyotaCorolla_Transformed_without_head.csv
 * 
 * 
 * Example: How to use the OLSMultipleLinearRegression class:
 * 
 * Instantiate an OLS regression object and load a dataset:
 * 
 * OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
 * double[] y = new double[]{11.0, 12.0, 13.0, 14.0, 15.0, 16.0};
 * double[][] x = new double[6][];
 * x[0] = new double[]{0, 0, 0, 0, 0};
 * x[1] = new double[]{2.0, 0, 0, 0, 0};
 * x[2] = new double[]{0, 3.0, 0, 0, 0};
 * x[3] = new double[]{0, 0, 4.0, 0, 0};
 * x[4] = new double[]{0, 0, 0, 5.0, 0};
 * x[5] = new double[]{0, 0, 0, 0, 6.0};          
 * regression.newSample(y, x);
 *
 * And after the model is built, you may examine regression parameters and diagnostics:
 *
 *   double[] beta = regression.estimateRegressionParameters();       
 *
 *   double[] residuals = regression.estimateResiduals();
 *
 *   double[][] parametersVariance = regression.estimateRegressionParametersVariance();
 * 
 *   double regressandVariance = regression.estimateRegressandVariance();
 *
 *   double rSquared = regression.calculateRSquared();
 *
 *   double sigma = regression.estimateRegressionStandardError();
 * 
 * @author Mahmoud Parsian (mahmoud.mparsian@yahoo.com)
 *
 */

public class OrdinaryLeastSquaresRegressionModel {
    
    static OLSMultipleLinearRegression buildModel(String trainingFilename, int TRAINING_SIZE) throws Exception {
        double[] Y = new double[TRAINING_SIZE];
        double[][] X = new double[TRAINING_SIZE][];  
        //
        // read file and build model
        //
        BufferedReader br = new BufferedReader(new FileReader(trainingFilename));
        try {
            //// prepare X and Y 
            String record;
            int index = 0;
            while ( (record = br.readLine()) != null) {
                // record: <Price><,><Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
                // tokens[0] = <Price>
                String[] tokens = StringUtils.split(record, ",");
                Y[index] = Double.parseDouble(tokens[0]);
                double[] features = new double[tokens.length - 1];
                for (int i = 0; i < features.length; i++) {
                    features[i] = Double.parseDouble(tokens[i+1]);
                } 
                X[index] = features;
                index++;
            }
        } 
        finally {
            br.close();
        }
            
        //
        // build model
        //
        OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();  
        // Loads model X and Y sample data, overriding any previous sample. 
        // Computes and caches QR decomposition of the X matrix.
        regression.newSampleData(Y, X);
        //regression.setNoIntercept(false);
        //regression.setNoIntercept(true);
        return regression;
    }

    /**
     * Predict using the built model
     * 
     * @param regression
     * @param x
     * @return 
     */
    static double predict(OLSMultipleLinearRegression regression, double[] x) {
        if (regression == null) {
            throw new IllegalArgumentException("regression must not be null.");
        }
        double[] beta = regression.estimateRegressionParameters();

        // intercept at beta[0]
        double prediction = beta[0];
        for (int i = 1; i < beta.length; i++) {
            prediction += beta[i] * x[i - 1];
        }
        //
        return prediction;
    }	         

}

