export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home
export BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
export SPARK_HOME=/Users/mparsian/spark-2.2.1
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
# build all other dependent jars in OTHER_JARS
JARS=`find $BOOK_HOME/lib -name '*.jar'  `
OTHER_JARS=""
for J in $JARS ; do 
   OTHER_JARS=$J,$OTHER_JARS
done
#

QUERY="file://$BOOK_HOME/src/main/java/org/dataalgorithms/machinelearning/logistic/spam/resources/query.txt"
MODEL_DIR="$BOOK_HOME/src/main/java/org/dataalgorithms/machinelearning/logistic/spam/resources/model"
MODEL="file://${MODEL_DIR}"

#
# remove all files under input
#$HADOOP_HOME/bin/hadoop fs -rmr $OUTPUT
#
# remove all files under output
driver=org.dataalgorithms.machinelearning.logistic.spam.EmailSpamDetection
$SPARK_HOME/bin/spark-submit --class $driver \
    --master local \
    --jars $OTHER_JARS \
    --conf "spark.yarn.jar=$SPARK_JAR" \
    $APP_JAR $QUERY $MODEL 
