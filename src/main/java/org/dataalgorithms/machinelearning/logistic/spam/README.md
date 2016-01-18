Machine Learning Algorithms using Spark: Logistic Regression
============================================================
The purpose of this package (solutions expressed in Java 
and Spark) is to show how to implement basic machine learning 
algorithms such as Logistic Regression in Spark and Spark's MLlib 
library.  Spark's MLlib offers a suite of machine learning 
libraries for Logistic Regression and other machine learning
algorithms. This example focuses on classifying emails into
two categories: spam and non-spam.

Training Data
=============
The training data is comprised of two sets:

* Spam Emails

* Non-spam Emails

Sample training data is provided here:

````
# ls -l resources/
-rw-r--r-- 1 ...  3362 ... emails_nospam.txt
-rw-r--r-- 1 ...  6010 ... emails_spam.txt
````

Put the training data into HDFS:

````
# hadoop fs -lsr /emails/
drwxr-xr-x   - ...          0 2015-12-28 15:13 /emails/nospam
-rw-r--r--   1 ...       3369 2015-12-28 15:13 /emails/nospam/emails_nospam.txt
drwxr-xr-x   - ...          0 2015-12-28 15:13 /emails/spam
-rw-r--r--   1 ...       6010 2015-12-28 15:13 /emails/spam/emails_spam.txt
````


Detect Spam and Non-Spam Emails
===============================  
This solution detects spam and non-spam emails

* org.dataalgorithms.machinelearning.logistic.EmailSpamDetectionBuildModel

The class ````EmailSpamDetectionBuildModel```` builds the model from the given training data

* org.dataalgorithms.machinelearning.logistic.EmailSpamDetection

The ````EmailSpamDetection```` is the driver class, which uses the built model 
to classify new queried data into two categories: spam and non-spam.


Build Model
===========
The class ````EmailSpamDetectionBuildModel```` reads the training data 
(spam and non-spam emails) and builds a ````LogisticRegressionModel```` and saves 
it in HDFS.

To build our logistic regression model, we use a shell script: ````run_build_model_on_yarn.sh````.
After running the shell script, the model is saved in HDFS as a parquet file.


````
# ./scripts/run_build_model_on_yarn.sh
````


After the run, we can examine the model:


````
# ./run_build_model_on_yarn.sh 
=== begin DEBUG-by-MP ===
DEBUG-by-MP: parameter p=org.apache.spark.deploy.SparkSubmit
DEBUG-by-MP: parameter p=--class
DEBUG-by-MP: parameter p=org.dataalgorithms.machinelearning.logistic.spam.EmailSpamDetectionBuildModel
DEBUG-by-MP: parameter p=--master
DEBUG-by-MP: parameter p=yarn-cluster
DEBUG-by-MP: parameter p=--jars
DEBUG-by-MP: parameter p=/Users/mparsian/zmp/github/data-algorithms-book/lib/spring-context-3.0.7.RELEASE.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/log4j-1.2.17.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/junit-4.12-beta-2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jsch-0.1.42.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/JeraAntTasks.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jedis-2.5.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jblas-1.2.3.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/hamcrest-all-1.3.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/guava-18.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-math3-3.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-math-2.2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-logging-1.1.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-lang3-3.4.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-lang-2.6.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-io-2.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-httpclient-3.0.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-daemon-1.0.5.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-configuration-1.6.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-collections-3.2.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-cli-1.2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/cloud9-1.3.2.jar,
DEBUG-by-MP: parameter p=--conf
DEBUG-by-MP: parameter p=spark.yarn.jar=/Users/mparsian/zmp/github/data-algorithms-book/lib/spark-assembly-1.5.2-hadoop2.5.0.jar
DEBUG-by-MP: parameter p=/Users/mparsian/zmp/github/data-algorithms-book/dist/data_algorithms_book.jar
DEBUG-by-MP: parameter p=/emails/spam
DEBUG-by-MP: parameter p=/emails/nospam
DEBUG-by-MP: parameter p=/emails/model

# hadoop fs -ls /emails/model/
drwxr-xr-x   - mparsian supergroup          0 2015-12-28 15:20 /emails/model/data
drwxr-xr-x   - mparsian supergroup          0 2015-12-28 15:20 /emails/model/metadata

# hadoop fs -lsr /emails/model/
drwxr-xr-x   - mparsian supergroup          0 2015-12-28 15:20 /emails/model/data
-rw-r--r--   1 mparsian supergroup          0 2015-12-28 15:20 /emails/model/data/_SUCCESS
-rw-r--r--   1 mparsian supergroup        939 2015-12-28 15:20 /emails/model/data/_common_metadata
-rw-r--r--   1 mparsian supergroup       1661 2015-12-28 15:20 /emails/model/data/_metadata
-rw-r--r--   1 mparsian supergroup       2472 2015-12-28 15:20 /emails/model/data/part-r-00000-35aa125d-4a67-4090-8f78-8658b6a39e17.gz.parquet
drwxr-xr-x   - mparsian supergroup          0 2015-12-28 15:20 /emails/model/metadata
-rw-r--r--   1 mparsian supergroup          0 2015-12-28 15:20 /emails/model/metadata/_SUCCESS
-rw-r--r--   1 mparsian supergroup        123 2015-12-28 15:20 /emails/model/metadata/part-00000

# hadoop fs -cat /emails/model/metadata/part-00000
{
 "class":"org.apache.spark.mllib.classification.LogisticRegressionModel",
 "version":"1.0",
 "numFeatures":100,
 "numClasses":2
}

# hadoop fs -copyToLocal  /emails/model/data/part-r-00000-35aa125d-4a67-4090-8f78-8658b6a39e17.gz.parquet  .

# ls -l
-rw-r--r--  1 mparsian  897801646  2472 Dec 28 15:27 part-r-00000-35aa125d-4a67-4090-8f78-8658b6a39e17.gz.parquet
-rwxr-xr-x@ 1 mparsian  897801646  1335 Dec 28 14:00 run_build_model_on_yarn.sh
-rwxr-xr-x@ 1 mparsian  897801646  1261 Dec 28 14:01 run_spam_detection_on_yarn.sh

# java org.dataalgorithms.parquet.TestConvertParquetToCSV part-r-00000-35aa125d-4a67-4090-8f78-8658b6a39e17.gz.parquet part.csv
15/12/28 15:29:28 INFO parquet.TestConvertParquetToCSV: parquetFile=part-r-00000-35aa125d-4a67-4090-8f78-8658b6a39e17.gz.parquet
15/12/28 15:29:28 INFO parquet.TestConvertParquetToCSV: csvOutputFile=part.csv
Dec 28, 2015 3:29:28 PM org.apache.parquet.Log info
INFO: Converting part-r-00000-35aa125d-4a67-4090-8f78-8658b6a39e17.gz.parquet to part.csv
15/12/28 15:29:29 INFO compress.CodecPool: Got brand-new decompressor [.gz]
15/12/28 15:29:29 INFO parquet.TestConvertParquetToCSV: estimatedTime (millis)=1197
Dec 28, 2015 3:29:29 PM INFO: org.apache.parquet.hadoop.ParquetFileReader: Initiating action with parallelism: 5
Dec 28, 2015 3:29:29 PM INFO: org.apache.parquet.hadoop.ParquetFileReader: reading another 1 footers
Dec 28, 2015 3:29:29 PM INFO: org.apache.parquet.hadoop.ParquetFileReader: Initiating action with parallelism: 5
Dec 28, 2015 3:29:29 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: RecordReader initialized will read a total of 1 records.
Dec 28, 2015 3:29:29 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: at row 0. reading next block
Dec 28, 2015 3:29:29 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: block read in memory in 67 ms. row count = 1

# cat part.csv
type: 1
values
  array: 0.05568034464109152
  array: 0.05698429765876554
  array: -0.26295822055468293
  array: 0.3855264708412645
  array: 0.11939611989777135
  array: 0.04034485170067723
  array: -0.032420287783533466
  array: -0.05703651743002959
  array: -0.10538576000199273
  array: -0.059944346496967914
  array: -0.006465579983037226
  array: 0.017746642180905207
  array: -0.2576425115786688
  array: 0.41618549963978846
  array: 0.21862767134488117
  array: 0.16328446238972416
  array: 0.10163607177157405
  array: 0.03210378995912523
  array: -0.35382223183901756
  array: 0.19716483970334586
  array: -0.07293980618692442
  array: -0.08686623631608327
  array: 0.33732657534065297
  array: -0.1120031819927968
  array: 0.16965247251754972
  array: 0.3095569552874886
  array: -0.15592977838573424
  array: 0.16191134277733202
  array: 0.17683152457232212
  array: 0.01751996924039695
  array: -0.21169371610559315
  array: -0.3688422842228063
  array: 0.297718352340347
  array: -0.1289847827642855
  array: 0.4575568838551987
  array: -0.0869830507336815
  array: 0.20988578294243557
  array: -0.45498448907345557
  array: -0.05387790599637403
  array: -0.41718678580354035
  array: 0.4860234266561518
  array: -0.3027034331178409
  array: -0.11327607130419932
  array: 0.502822920553092
  array: 0.18188123406044068
  array: 0.09855437148920858
  array: 0.29430701281454236
  array: -0.2301853605106512
  array: -0.3373871661543336
  array: -0.004310438882151541
  array: 0.23035257691988
  array: -0.09859371044757746
  array: 0.7230437568240142
  array: 0.21124421565388357
  array: -0.3535180528637035
  array: 0.39214647731137364
  array: -0.02752877603963021
  array: -0.18666731610240697
  array: 0.1075615894405431
  array: -0.1253227645025999
  array: 0.19503660095563294
  array: 0.021947552812720796
  array: 0.042967853958067126
  array: -0.17338159231365025
  array: 0.08715440550598386
  array: -0.16573276335212778
  array: -0.1771622331027798
  array: 0.5481865107356374
  array: -0.19055118238976324
  array: -0.016287571422762593
  array: 0.29501380986259745
  array: -0.03954042357191635
  array: 0.3558656495055171
  array: -0.3434012647949229
  array: -0.08372941531391931
  array: 0.09098900820523355
  array: 0.10258866135722738
  array: -0.7799867702413525
  array: -0.06340049472947029
  array: -0.07332375311593112
  array: 0.44426450264916206
  array: -0.3600231700915972
  array: 0.14586281355167727
  array: -0.22608122844113918
  array: -0.16666522800494765
  array: -0.4037539646175289
  array: -0.11652204515866152
  array: 0.07600425612268216
  array: 0.46208160027339307
  array: 0.02856609830560284
  array: 0.2060913028522841
  array: -0.10426492610141183
  array: 0.006899500317060889
  array: -0.7087623723726486
  array: 0.09651437664063575
  array: -0.06683311081457143
  array: -0.16190126615589642
  array: 0.009492041067237165
  array: -0.23581817842957262
  array: -0.17783091970720147
|0.0|0.5
````

Using LogisticRegressionModel
=============================
To use the built model, we have prepared 8 query records:


````
mparsian@Mahmouds-MacBook:~/zmp/github/data-algorithms-book/src/main/java/org/dataalgorithms/machinelearning/logistic/spam# cat resources/query.txt 
this is a year of promotion for Galaxy End of YearPromo You have 1 week remaining to retrieve your won prize for the Samsung Galaxy Xmas Promo 'C' draw category winning prize of Seven Hundred and Fifty Thousand Euros each and a Samsung Galaxy S6 EDGE. Winning Ticket Number:WIN-707-COS.  We advise you to keep this winning notification confidential and away from public notice to avoid double claim/mistransfer or impersonation until after remittance/payment to you.
you are the lucky one: We've picked out 10 new matches for you. Meet them now and then check out all the singles in your area! you might win a prize too
Do not miss your chances: Get Viagra real cheap!  Send money right away to ...
Get real money fast: With my position in the office i assure you with 100% risk free that this transaction is not a childish game play and i want you to indicate your full interest with assurance of trust that you will not betray me once the fund is transfer into your nominated bank account, while i look forward for your urgent reply.
Dear Spark Learner, Thanks so much for attending the Spark Summit 2014!  Check out videos of talks from the summit at ...
Hi Mom, Apologies for being late about emailing and forgetting to send you the package.  I hope you and bro have been ...
Wow, hey Fred, just heard about the Spark petabyte sort.  I think we need to take time to try it out immediately ...
Hi Spark user list, This is my first question to this list, so thanks in advance for your help!  I tried running ...
````

where the first 4 records are spam and the last 4 records are non-spam;

````
# ./run_spam_detection_on_yarn.sh 
=== begin DEBUG-by-MP ===
DEBUG-by-MP: parameter p=org.apache.spark.deploy.SparkSubmit
DEBUG-by-MP: parameter p=--class
DEBUG-by-MP: parameter p=org.dataalgorithms.machinelearning.logistic.spam.EmailSpamDetection
DEBUG-by-MP: parameter p=--master
DEBUG-by-MP: parameter p=yarn-cluster
DEBUG-by-MP: parameter p=--jars
DEBUG-by-MP: parameter p=/Users/mparsian/zmp/github/data-algorithms-book/lib/spring-context-3.0.7.RELEASE.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/log4j-1.2.17.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/junit-4.12-beta-2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jsch-0.1.42.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/JeraAntTasks.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jedis-2.5.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jblas-1.2.3.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/hamcrest-all-1.3.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/guava-18.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-math3-3.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-math-2.2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-logging-1.1.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-lang3-3.4.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-lang-2.6.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-io-2.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-httpclient-3.0.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-daemon-1.0.5.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-configuration-1.6.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-collections-3.2.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-cli-1.2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/cloud9-1.3.2.jar,
DEBUG-by-MP: parameter p=--conf
DEBUG-by-MP: parameter p=spark.yarn.jar=/Users/mparsian/zmp/github/data-algorithms-book/lib/spark-assembly-1.5.2-hadoop2.5.0.jar
DEBUG-by-MP: parameter p=/Users/mparsian/zmp/github/data-algorithms-book/dist/data_algorithms_book.jar
DEBUG-by-MP: parameter p=/emails/query
DEBUG-by-MP: parameter p=/emails/model
=== done DEBUG-by-MP ===
````

Output from EmailSpamDetection Run
=====================================
As you can see from the output, the built model predicted all 8 email records correctly!!!
The following output is from the Spark logs.

````
15/12/28 15:42:48 INFO spam.EmailSpamDetection: query email=this is a year of promotion for Galaxy End of YearPromo You have 1 week remaining to retrieve your won prize for the Samsung Galaxy Xmas Promo 'C' draw category winning prize of Seven Hundred and Fifty Thousand Euros each and a Samsung Galaxy S6 EDGE. Winning Ticket Number:WIN-707-COS.  We advise you to keep this winning notification confidential and away from public notice to avoid double claim/mistransfer or impersonation until after remittance/payment to you.
15/12/28 15:42:48 INFO spam.EmailSpamDetection: prediction=1.0
15/12/28 15:42:48 INFO spam.EmailSpamDetection: query email=you are the lucky one: We've picked out 10 new matches for you. Meet them now and then check out all the singles in your area! you might win a prize too
15/12/28 15:42:48 INFO spam.EmailSpamDetection: prediction=1.0
15/12/28 15:42:48 INFO spam.EmailSpamDetection: query email=Do not miss your chances: Get Viagra real cheap!  Send money right away to ...
15/12/28 15:42:48 INFO spam.EmailSpamDetection: prediction=1.0
15/12/28 15:42:48 INFO spam.EmailSpamDetection: query email=Get real money fast: With my position in the office i assure you with 100% risk free that this transaction is not a childish game play and i want you to indicate your full interest with assurance of trust that you will not betray me once the fund is transfer into your nominated bank account, while i look forward for your urgent reply.
15/12/28 15:42:48 INFO spam.EmailSpamDetection: prediction=1.0
15/12/28 15:42:48 INFO spam.EmailSpamDetection: query email=Dear Spark Learner, Thanks so much for attending the Spark Summit 2014!  Check out videos of talks from the summit at ...
15/12/28 15:42:48 INFO spam.EmailSpamDetection: prediction=0.0
15/12/28 15:42:48 INFO spam.EmailSpamDetection: query email=Hi Mom, Apologies for being late about emailing and forgetting to send you the package.  I hope you and bro have been ...
15/12/28 15:42:48 INFO spam.EmailSpamDetection: prediction=0.0
15/12/28 15:42:48 INFO spam.EmailSpamDetection: query email=Wow, hey Fred, just heard about the Spark petabyte sort.  I think we need to take time to try it out immediately ...
15/12/28 15:42:48 INFO spam.EmailSpamDetection: prediction=0.0
15/12/28 15:42:48 INFO spam.EmailSpamDetection: query email=Hi Spark user list, This is my first question to this list, so thanks in advance for your help!  I tried running ...
15/12/28 15:42:48 INFO spam.EmailSpamDetection: prediction=0.0
````