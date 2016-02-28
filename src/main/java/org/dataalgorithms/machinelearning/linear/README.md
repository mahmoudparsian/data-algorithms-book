Linear Regression
=================
Linear regression analysis is the study of linear relationships between variables.  

In simplest forms, a linear regression line has an equation of the form 
````
    Y = a + bX
````
where 

* X is the explanatory variable
* Y is the dependent variable
* b is the slope of the line
* a is the intercept (the value of y when x = 0)


The following figure shows a dataset of many ````(x,y)```` points, 
plotted so that ````Y````, the variable we would like to be able to 
predict -- the vertical axis --, and a single independent variable 
````X```` -- the horizontal axis.  The goal of the linear regression 
analysis is to find the linear function that provides the best 
prediction of ````Y````. (source:  [About Aspatial Linear Regression](https://www.biomedware.com/files/documentation/spacestat/Statistics/Multivariate_Modeling/Regression/About_Aspatial_Linear_Regression.htm))

![Linear Regression in Picture](https://raw.githubusercontent.com/mahmoudparsian/data-algorithms-book/master/misc/linear_regression_line.png)


Machine Learning vs. Traditional Programming
============================================

![Machine Learning in Pictures](https://raw.githubusercontent.com/mahmoudparsian/data-algorithms-book/master/misc/machine_learning.jpg)


Machine Learning Algorithms using Spark: Linear Regression
==========================================================
The purpose of this package (solutions expressed in Java 
and Spark) is to show how to implement basic machine learning 
algorithms such as Linear Regression in Spark and Spark's MLlib 
library.  Spark's MLlib offers a suite of machine learning 
libraries for Linear Regression and other machine learning
algorithms. This example focuses on predicting car prices 
using 10 car features (such as Price, Age ,FuelType, ...).
Not that the model we build can be used only for type of cars 
(since the training data is only for Toyota Corolla prices).

Given that we are using a linear regression model, we are 
assuming the relationship between the independent and 
dependent variables follow a (almost) straight line.

We will use a simple example (predicting car prices) to demonstrate 
3 different solutions:

* Pure POJO Solution: implement ordinary least squares (OLS) to estimate the 
  parameters of a multiple linear regression model. This solution is a traditional 
  and sequential solution (no MapReduce here) and is based on the 
  class ````org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression````.
  The POJO solution is presented in ````org.dataalgorithms.machinelearning.linear.OLS```` 
  package.

  
* R Solution: use ````lm````

* Spark Solution using stochastic gradient descent method:  ````org.apache.spark.mllib.regression.LinearRegressionWithSGD````.
  The Spark solution is presented in ````org.dataalgorithms.machinelearning.linear.SGD````  package.
  

Example Description: Predicting Car Prices
==========================================
This example is based on the blog [Predicting Car Prices Part 1: Linear Regression by Peter Chen](http://www.datasciencecentral.com/profiles/blogs/predicting-car-prices-part-1-linear-regression).
The purpose of this exercise is to predict Toyota Corolla car prices 
by using regression model (by 3 different solutions: POJO, R, and Spark). 


Training Data
=============
To build a ````LinearRegressionModel````, we need a training data set.
The [training data](https://raw.githubusercontent.com/datailluminations/PredictingToyotaPricesBlog/master/ToyotaCorolla.csv) is 
available from the GitHub.

The training data set is for Toyota Corolla cars and each record has
the following format:

````
<Price><,><Age><,><KM><,><FuelType><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
````

I have downloaded the training data and saved as: ToyotaCorolla.csv.
Let's examine the training data set:

````
head -5 ToyotaCorolla.csv 
Price,Age,KM,FuelType,HP,MetColor,Automatic,CC,Doors,Weight
13500,23,46986,Diesel,90,1,0,2000,3,1165
13750,23,72937,Diesel,90,1,0,2000,3,1165
13950,24,41711,Diesel,90,1,0,2000,3,1165
14950,26,48000,Diesel,90,0,0,2000,3,1165
````

The training data is located at ````resources/ToyotaCorolla.csv````


Data Transformation & Featurization
===================================
Since all of the training data attributes are not numeric, we do need to 
transform non-numeric data (such FuelType) into a numeric value.  For our 
training data, we need to convert the categorical variables (such as FuelType) 
to numeric variables to feed into Spark's linear regression model, because 
linear regression models only take numeric variables.

In the training data, there are 3 "Fuel Types": 1) CNG 2) Diesel 3) Petrol


FuelType | Frequency in the Training Data Set
---------|-----------------------------------
CNG      |    17
Diesel   |   155
Petrol   |  1264

According to [Peter Chen's blog](http://www.datasciencecentral.com/profiles/blogs/predicting-car-prices-part-1-linear-regression):

````
  "So, we  can  convert  the categorical variable Fuel 
  Type to two numeric variables: FuelType1 and FuelType2. 
  We assign CNG to a new variable FuelType1 in which a 1 
  represents it’s a CNG vehicle and 0 it’s not. Likewise, 
  we assign Diesel to a new variable FuelType2 in which 
  a 1 represents it’s a Diesel vehicle and 0 it’s not. 
  So, what  do  we do  with PETROL vehicles? This  is 
  represented  by  the  case  when BOTH FuelType1 and 
  FuelType2 are zero."
````


Therefore, we create a trsnformed training data, by substituting "CNG" 
to "1", "Diesel" to "1" and "Petrol" to "0". The transformed training 
data is saved as 

FuelType | FuelType1 | FuelType2
---------|-----------|----------
CNG      | 1         | 0
Diesel   | 0         | 1
Petrol   | 0         | 0


Therefore, the transformed training data set for Toyota Corolla cars 
will have the following format:

````
<Price><,><Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
````

Data Transformation Script
==========================
An AWK script is used to generate the transformed data:

````
awk -f ./scripts/transform.awk ./resources/ToyotaCorolla.csv > ./resources/ToyotaCorolla_Transformed.csv
````

To feed the the data to Spark's Linear Regression, we removed the headline from the  ````ToyotaCorolla_Transformed.csv````.


The training data without headline is labeled as: ````ToyotaCorolla_Transformed_without_head.csv````



Spark Solution: Predicting Car Prices
=====================================
This example is based on the blog [Predicting Car Prices Part 1: Linear 
Regression by Peter Chen](http://www.datasciencecentral.com/profiles/blogs/predicting-car-prices-part-1-linear-regression).
The purpose of this exercise is to predict Toyota Corolla car prices 
by using Spark's MLlib linear regression model, ````org.apache.spark.mllib.regression.LinearRegressionModel```` 
(regression model trained using ````org.apache.spark.mllib.regression.LinearRegressionWithSGD````
-- train a linear regression model with no regularization using Stochastic Gradient Descent).


The following Spark programs predict car prices using Linear Regression model 

* org.dataalgorithms.machinelearning.linear.CarPricePredictionBuildModel

The class ````CarPricePredictionBuildModel```` builds the model from the given training data


* org.dataalgorithms.machinelearning.linear.CarPricePrediction

This is the driver class, which uses the built model to predict new queried cars


* org.dataalgorithms.machinelearning.linear.TestAccuracyOfModel

This is the test class, which tests the accuracy of the built model for predicting new queried cars.
We will test the model against the training data, which the model is built from originally.



Spark Solution: Build Model
===========================
The class ````CarPricePredictionBuildModel```` reads the training data 
and builds a ````LinearRegressionModel```` and saves it in HDFS.



To build our linear regression model, we use a shell script: ````run_build_model_on_yarn.sh````.  
After running the shell script, the model is saved in HDFS as a parquet file:

````
$ ./scripts/run_build_model_on_yarn.sh
````


Spark Solution: Examine the Built Model
=======================================

````
$ hadoop fs -lsr /car/model
drwxr-xr-x   - ...          0 2016-01-01 00:20 /car/model/data
-rw-r--r--   1 ...          0 2016-01-01 00:20 /car/model/data/_SUCCESS
-rw-r--r--   1 ...        856 2016-01-01 00:20 /car/model/data/_common_metadata
-rw-r--r--   1 ...       1454 2016-01-01 00:20 /car/model/data/_metadata
-rw-r--r--   1 ...       1518 2016-01-01 00:20 /car/model/data/part-r-00000-1d042c3a-c922-4623-a63c-a205eb791c5b.gz.parquet
drwxr-xr-x   - ...          0 2016-01-01 00:20 /car/model/metadata
-rw-r--r--   1 ...          0 2016-01-01 00:20 /car/model/metadata/_SUCCESS
-rw-r--r--   1 ...        100 2016-01-01 00:20 /car/model/metadata/part-00000

$ hadoop fs -copyToLocal /car/model/data/part-r-00000-1d042c3a-c922-4623-a63c-a205eb791c5b.gz.parquet .

$ java org.dataalgorithms.parquet.TestConvertParquetToCSV part-r-00000-1d042c3a-c922-4623-a63c-a205eb791c5b.gz.parquet part.csv
16/01/01 00:22:26 INFO parquet.TestConvertParquetToCSV: parquetFile=part-r-00000-1d042c3a-c922-4623-a63c-a205eb791c5b.gz.parquet
16/01/01 00:22:26 INFO parquet.TestConvertParquetToCSV: csvOutputFile=part.csv
Jan 01, 2016 12:22:26 AM org.apache.parquet.Log info
INFO: Converting part-r-00000-1d042c3a-c922-4623-a63c-a205eb791c5b.gz.parquet to part.csv
16/01/01 00:22:27 INFO compress.CodecPool: Got brand-new decompressor [.gz]
16/01/01 00:22:27 INFO parquet.TestConvertParquetToCSV: estimatedTime (millis)=1157
Jan 1, 2016 12:22:27 AM INFO: org.apache.parquet.hadoop.ParquetFileReader: Initiating action with parallelism: 5
Jan 1, 2016 12:22:27 AM INFO: org.apache.parquet.hadoop.ParquetFileReader: reading another 1 footers
Jan 1, 2016 12:22:27 AM INFO: org.apache.parquet.hadoop.ParquetFileReader: Initiating action with parallelism: 5
Jan 1, 2016 12:22:27 AM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: RecordReader initialized will read a total of 1 records.
Jan 1, 2016 12:22:27 AM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: at row 0. reading next block
Jan 1, 2016 12:22:27 AM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: block read in memory in 67 ms. row count = 1

$ cat part.csv
type: 1
values
  array: 8.821458794225991E-4
  array: 0.10696700124271294
  array: -8.710008912993249E-7
  array: 0.0035096159746738284
  array: 2.413584165230031E-5
  array: 2.6627598119420613E-6
  array: 0.048026609311824286
  array: 1.332468926516163E-4
  array: 0.0
|0.0



$ hadoop fs -cat /car/model/metadata/part-00000
{
 "class":"org.apache.spark.mllib.regression.LinearRegressionModel",
 "version":"1.0",
 "numFeatures":9
}
````

Spark Solution: Using LinearRegressionModel
===========================================
To use the built model, we have prepared 8 query records:

````
# cat resources/query.txt 
23,46986,1,90,1,0,2000,3,1165
23,72937,1,90,1,0,2000,3,1165
24,41711,1,90,1,0,2000,3,1165
26,48000,1,90,0,0,2000,3,1165
30,38500,1,90,0,0,2000,3,1170
32,61000,1,90,0,0,2000,3,1170
27,94612,1,90,1,0,2000,3,1245
30,75889,1,90,1,0,2000,3,1245
````


To run our query data against the built model, we use a shell script: ````run_car_price_prediction_on_yarn.sh````

````
# ./scripts/run_car_price_prediction_on_yarn.sh
16/01/01 00:59:26 INFO linear.CarPricePrediction: query record=23,46986,1,90,1,0,2000,3,1165
16/01/01 00:59:26 INFO linear.CarPricePrediction: carPricePrediction=5122.341316812225
16/01/01 00:59:26 INFO linear.CarPricePrediction: query record=23,72937,1,90,1,0,2000,3,1165
16/01/01 00:59:26 INFO linear.CarPricePrediction: carPricePrediction=7898.241966061869
16/01/01 00:59:26 INFO linear.CarPricePrediction: query record=24,41711,1,90,1,0,2000,3,1165
16/01/01 00:59:26 INFO linear.CarPricePrediction: carPricePrediction=4558.091267402794
16/01/01 00:59:26 INFO linear.CarPricePrediction: query record=26,48000,1,90,0,0,2000,3,1165
16/01/01 00:59:26 INFO linear.CarPricePrediction: carPricePrediction=5230.8084783741315
16/01/01 00:59:26 INFO linear.CarPricePrediction: query record=30,38500,1,90,0,0,2000,3,1170
16/01/01 00:59:26 INFO linear.CarPricePrediction: carPricePrediction=4214.625495151878
16/01/01 00:59:26 INFO linear.CarPricePrediction: query record=32,61000,1,90,0,0,2000,3,1170
16/01/01 00:59:26 INFO linear.CarPricePrediction: carPricePrediction=6621.384787404677
16/01/01 00:59:26 INFO linear.CarPricePrediction: query record=27,94612,1,90,1,0,2000,3,1245
16/01/01 00:59:26 INFO linear.CarPricePrediction: carPricePrediction=10216.75524658119
16/01/01 00:59:26 INFO linear.CarPricePrediction: query record=30,75889,1,90,1,0,2000,3,1245
16/01/01 00:59:26 INFO linear.CarPricePrediction: carPricePrediction=8214.014728751514

The correct prices are:
13500,23,46986,1,90,1,0,2000,3,1165
13750,23,72937,1,90,1,0,2000,3,1165
13950,24,41711,1,90,1,0,2000,3,1165
14950,26,48000,1,90,0,0,2000,3,1165
13750,30,38500,1,90,0,0,2000,3,1170
12950,32,61000,1,90,0,0,2000,3,1170
16900,27,94612,1,90,1,0,2000,3,1245
18600,30,75889,1,90,1,0,2000,3,1245

````

Spark Solution: Output from ModelEvaluation
===========================================
````
args[0]=/car/training
args[1]=/car/model
Training Mean Squared Error = 5.686839313236483E7
````

R Solution
==========

````
R
> auto <- read.csv(file="ToyotaCorolla_Transformed.csv",head=TRUE,sep=",")
> attributes(auto)
$names
 [1] "Price"     "Age"       "KM"        "FuelType1" "FuelType2" "HP"
 [7] "MetColor"  "Automatic" "CC"        "Doors"     "Weight"
...


> model <- lm(formula = Price ~ ., data = auto)
> model

Call:
lm(formula = Price ~ ., data = auto)

Coefficients:
(Intercept)          Age           KM    FuelType1    FuelType2           HP
 -2.681e+03   -1.220e+02   -1.621e-02   -1.121e+03    2.269e+03    6.081e+01
   MetColor    Automatic           CC        Doors       Weight
  5.716e+01    3.303e+02   -4.174e+00   -7.776e+00    2.001e+01
````


We	modeled	price	as	a	 function	of	10 features(Age, KM, ...),
taken	 from	the	data frame ````auto```` and we saved this model into 
an object that we named ````model````.	To	see	what the linear model did, 
we have to “summarize” this object using the function summary():

````
> summary(model)

Call:
lm(formula = Price ~ ., data = auto)

Residuals:
     Min       1Q   Median       3Q      Max
-10642.3   -737.7      3.1    731.3   6451.5

Coefficients:
              Estimate Std. Error t value Pr(>|t|)
(Intercept) -2.681e+03  1.219e+03  -2.199 0.028036 *
Age         -1.220e+02  2.602e+00 -46.889  < 2e-16 ***
KM          -1.621e-02  1.313e-03 -12.347  < 2e-16 ***
FuelType1   -1.121e+03  3.324e+02  -3.372 0.000767 ***
FuelType2    2.269e+03  4.394e+02   5.164 2.75e-07 ***
HP           6.081e+01  5.756e+00  10.565  < 2e-16 ***
MetColor     5.716e+01  7.494e+01   0.763 0.445738
Automatic    3.303e+02  1.571e+02   2.102 0.035708 *
CC          -4.174e+00  5.453e-01  -7.656 3.53e-14 ***
Doors       -7.776e+00  4.006e+01  -0.194 0.846129
Weight       2.001e+01  1.203e+00  16.629  < 2e-16 ***
---
Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1

Residual standard error: 1316 on 1425 degrees of freedom
Multiple R-squared:  0.8693,	Adjusted R-squared:  0.8684
F-statistic:   948 on 10 and 1425 DF,  p-value: < 2.2e-16
````

Output of ````symmary(model)````: First,	 you’re being reminded of the model 
formula that we	entered. Then, the model gives us the residuals and the 
coefficients of the fixed effects; then, the output prints some overall 
results of the model that we constructed.


Let's work through this output. Let’s start with “Multiple R-squared:  0.8693”. 
This refers to the statistic R<sup>2</sup> which is a measure of “variance explained” 
or it is a measure of “variance accounted for”. R<sup>2</sup> values range from 0.00
to 1.00. Our R<sup>2</sup> is 0.8693, which is quite high and you can interpret this 
as showing that	0.8693%	of the stuff that’s happening in our dataset is “explained” by
our	linear regression model.	

In general, we	want R<sup>2</sup> values to be	 high, but what is considered a high 
R<sup>2</sup> value depends on your field and on your training data.

Plotting out linear model:

````
> jpeg('rplot.jpg')
> plot(fitted(model),residuals(model))
> dev.off()
````

![Plotting the Linear Model](https://raw.githubusercontent.com/mahmoudparsian/data-algorithms-book/master/misc/rplot.jpg)

R Solution: Predict Price of a Car
----------------------------------
````
Data from Training: 13500,23,46986,0,1,90,1,0,2000,3,1165
> Age = c(23)
> KM = c(46986)
> FuelType1 = c(0)
> FuelType2 = c(1)
> HP = c(90)
> MetColor = c(1)
> Automatic = c(0)
> CC = c(2000)
> Doors = c(3)
> Weight = c(1165)
> query = data.frame(Age,KM,FuelType1,FuelType2,HP,MetColor,Automatic,CC,Doors,Weight)
> predict(model, query)
    1
16490


Data from Training: 20950,25,31461,0,0,192,0,0,1800,3,1185
> Age = c(25)
> KM = c(31461)
> FuelType1 = c(0)
> FuelType2 = c(0)
> HP = c(192)
> MetColor = c(0)
> Automatic = c(0)
> CC = c(1800)
> Doors = c(3)
> Weight = c(1185)
> query = data.frame(Age,KM,FuelType1,FuelType2,HP,MetColor,Automatic,CC,Doors,Weight)
> predict(fit, query)
       1
21609.06

````

POJO Solution
=============
The POJO solution is using the ````org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression```` 
class. The class ````OLSMultipleLinearRegression```` implements ordinary least squares (OLS) to estimate the parameters of a multiple linear regression model.
The solution is comprised of two classes:


*  ````OrdinaryLeastSquaresRegressionModel````: builds the linear regression model using the class ````OLSMultipleLinearRegression````
 
*  ````OrdinaryLeastSquaresRegressionDriver````: reads training data, builds the model, and finally uses the query data to predict car prices.



````
export TRAINING_SIZE=1436
export TRAINING_DATA=ToyotaCorolla_Transformed_without_head.csv
export QUERY_DATA=query.txt
export prog=org.dataalgorithms.machinelearning.linear.OrdinaryLeastSquaresRegressionDriver
java $prog 1436 $TRAINING_DATA $QUERY_DATA
queryRecord=23,46986,0,1,90,1,0,2000,3,1165
predicted price=16490.00034792102
queryRecord=23,72937,0,1,90,1,0,2000,3,1165
predicted price=16069.378248572904
queryRecord=24,41711,0,1,90,1,0,2000,3,1165
predicted price=16453.484747133152
queryRecord=26,48000,0,1,90,0,0,2000,3,1165
predicted price=16050.361881282868
queryRecord=30,38500,0,1,90,0,0,2000,3,1170
predicted price=15816.329755096343
queryRecord=32,61000,0,1,90,0,0,2000,3,1170
predicted price=15207.613594041
queryRecord=27,94612,0,1,90,1,0,2000,3,1245
predicted price=16830.753509468013
queryRecord=30,75889,0,1,90,1,0,2000,3,1245
predicted price=16768.178417015813
queryRecord=27,19700,0,0,192,0,0,1800,3,1185
predicted price=21555.658909725575
queryRecord=23,71138,0,1,69,0,0,1900,3,1105
predicted price=13981.174150814048
````

Questions/Comments
==================
* [View Mahmoud Parsian's profile on LinkedIn](http://www.linkedin.com/in/mahmoudparsian)
* Please send me an email: mahmoud.parsian@yahoo.com
* [Twitter: @mahmoudparsian](http://twitter.com/mahmoudparsian) 

Thank you!
````
best regards,
Mahmoud Parsian
````

[![Data Algorithms Book](https://raw.githubusercontent.com/mahmoudparsian/data-algorithms-book/master/misc/data_algorithms_image.jpg)](http://shop.oreilly.com/product/0636920033950.do)
