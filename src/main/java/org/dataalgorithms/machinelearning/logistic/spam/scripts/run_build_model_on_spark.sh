export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home
export BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
export SPARK_HOME=/Users/mparsian/spark-2.2.1
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
# build all other dependent jars in OTHER_JARS
JARS=`find $BOOK_HOME/lib -name '*.jar' `
OTHER_JARS=""
for J in $JARS ; do 
   OTHER_JARS=$J,$OTHER_JARS
done
#


# define input/output for Hadoop/HDFS
SPAM_TRAINING="file://$BOOK_HOME/src/main/java/org/dataalgorithms/machinelearning/logistic/spam/resources/emails_spam.txt"
NON_SPAM_TRAINING="file://$BOOK_HOME/src/main/java/org/dataalgorithms/machinelearning/logistic/spam/resources/emails_nospam.txt"
MODEL_DIR="$BOOK_HOME/src/main/java/org/dataalgorithms/machinelearning/logistic/spam/resources/model"
MODEL="file://${MODEL_DIR}"
#
# remove all files under input
rm -fr ${MODEL_DIR}
#
# remove all files under output
driver=org.dataalgorithms.machinelearning.logistic.spam.EmailSpamDetectionBuildModel
$SPARK_HOME/bin/spark-submit --class $driver \
    --master local \
    --jars $OTHER_JARS \
    --conf "spark.yarn.jar=$SPARK_JAR" \
    $APP_JAR $SPAM_TRAINING $NON_SPAM_TRAINING $MODEL
