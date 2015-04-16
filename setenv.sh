# This is an example to set 3 environment variables
# You should update your script accrodingly
#
#set Java as jdk7

# macbook:
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home

# linux:
# export JAVA_HOME=/usr/java/jdk7

echo "JAVA_HOME=$JAVA_HOME"
#
# set ant 
export ANT_HOME=/Users/mparsian/zmp/zs/apache-ant-1.9.4
echo "ANT_HOME=$ANT_HOME"
#
# set PATH
export PATH=$JAVA_HOME/bin:$ANT_HOME/bin:$PATH
echo "PATH=$PATH"
#
BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
jars=`find $BOOK_HOME/lib -name '*.jar'`
for j in $jars ; do
	CLASSPATH=$CLASSPATH:$j
done
#
CLASSPATH=$CLASSPATH:$BOOK_HOME/dist/data_algorithms_book.jar
#
export CLASSPATH=$CLASSPATH
