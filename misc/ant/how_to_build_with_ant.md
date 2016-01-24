How To Build with Apache Ant
============================
Before you build, you should 
read [README_lib.md](https://github.com/mahmoudparsian/data-algorithms-book/blob/master/misc/README_lib.md)

Here I am assuming that your install directory 
is ````<install-dir-data-algorithms-book>/```` 
and you are executing build instructions from 
this directory.

[Apache's ant 1.9.4](http://ant.apache.org/) is used for building the project.

Before you build, you do need to define the following (please edit these directories accordingly):

    # This is an example to set 3 environment variables
    # You should update your script accrodingly
    #
    #set Java as jdk7
    export JAVA_HOME=/usr/java/jdk7
    echo "JAVA_HOME=$JAVA_HOME"
    #
    # set ant 
    export ANT_HOME=/home/mp/apache-ant-1.9.4
    echo "ANT_HOME=$ANT_HOME"
    #
    # set PATH
    export PATH=$JAVA_HOME/bin:$ANT_HOME/bin:$PATH
    echo "PATH=$PATH"


* To clean up:

  **ant clean**

* To build: the build will create 
````<install-dir-data-algorithms-book>/dist/data_algorithms_book.jar```` file.

  **ant**

* To check your build environment:

  **ant  myenv**


Sample Builds
=============
````
$ ant clean
Buildfile: build.xml

clean:

BUILD SUCCESSFUL
Total time: 0 seconds

$ ant
Buildfile: build.xml

init:
    [mkdir] Created dir: /home/mp/data-algorithms-book/build
    [mkdir] Created dir: /home/mp/data-algorithms-book/dist
    [mkdir] Created dir: /home/mp/data-algorithms-book/reports
    [mkdir] Created dir: /home/mp/data-algorithms-book/reports/raw
    [mkdir] Created dir: /home/mp/data-algorithms-book/reports/html

check-spark-jar:

do-copy-jar:
     [echo] copying spark-assembly-1.5.2-hadoop2.6.0.jar...
     [copy] Copying 1 resource to /home/mp/data-algorithms-book/lib


build_jar:
     [echo] javac
     [echo] compiling src...
    [javac] Compiling 173 source files to /home/mp/data-algorithms-book/build
    [javac] warning: [options] bootstrap class path not set in conjunction with -source 1.7
    [javac] Note: Some input files use or override a deprecated API.
    [javac] Note: Recompile with -Xlint:deprecation for details.
    [javac] Note: Some input files use unchecked or unsafe operations.
    [javac] Note: Recompile with -Xlint:unchecked for details.
    [javac] 1 warning
      [jar] Building jar: /home/mp/data-algorithms-book/dist/data_algorithms_book.jar

BUILD SUCCESSFUL
Total time: 1 minute 29 seconds

$ ant
Buildfile: build.xml

init:

check-spark-jar:

do-copy-jar:

build_jar:
     [echo] javac
     [echo] compiling src...
    [javac] Compiling 173 source files to /home/mp/data-algorithms-book/build
    [javac] warning: [options] bootstrap class path not set in conjunction with -source 1.7
    [javac] Note: Some input files use or override a deprecated API.
    [javac] Note: Recompile with -Xlint:deprecation for details.
    [javac] Note: Some input files use unchecked or unsafe operations.
    [javac] Note: Recompile with -Xlint:unchecked for details.
    [javac] 1 warning
      [jar] Building jar: /home/mp/data-algorithms-book/dist/data_algorithms_book.jar

BUILD SUCCESSFUL
Total time: 3 seconds

$ ant clean
Buildfile: build.xml

clean:
   [delete] Deleting directory /home/mp/data-algorithms-book/build
   [delete] Deleting directory /home/mp/data-algorithms-book/dist
   [delete] Deleting directory /home/mp/data-algorithms-book/reports

BUILD SUCCESSFUL
Total time: 0 seconds
mparsian@Mahmouds-MacBook:~/zmp/map_reduce_book/data-algorithms-book# ant
Buildfile: build.xml

init:
    [mkdir] Created dir: /home/mp/data-algorithms-book/build
    [mkdir] Created dir: /home/mp/data-algorithms-book/dist
    [mkdir] Created dir: /home/mp/data-algorithms-book/reports
    [mkdir] Created dir: /home/mp/data-algorithms-book/reports/raw
    [mkdir] Created dir: /home/mp/data-algorithms-book/reports/html

check-spark-jar:

do-copy-jar:

build_jar:
     [echo] javac
     [echo] compiling src...
    [javac] Compiling 173 source files to /home/mp/data-algorithms-book/build
    [javac] warning: [options] bootstrap class path not set in conjunction with -source 1.7
    [javac] Note: Some input files use or override a deprecated API.
    [javac] Note: Recompile with -Xlint:deprecation for details.
    [javac] Note: Some input files use unchecked or unsafe operations.
    [javac] Note: Recompile with -Xlint:unchecked for details.
    [javac] 1 warning
      [jar] Building jar: /home/mp/data-algorithms-book/dist/data_algorithms_book.jar

BUILD SUCCESSFUL
Total time: 3 seconds

$ ant
Buildfile: build.xml

init:

check-spark-jar:

do-copy-jar:

build_jar:
     [echo] javac
     [echo] compiling src...
    [javac] Compiling 173 source files to /home/mp/data-algorithms-book/build
    [javac] warning: [options] bootstrap class path not set in conjunction with -source 1.7
    [javac] Note: Some input files use or override a deprecated API.
    [javac] Note: Recompile with -Xlint:deprecation for details.
    [javac] Note: Some input files use unchecked or unsafe operations.
    [javac] Note: Recompile with -Xlint:unchecked for details.
    [javac] 1 warning
      [jar] Building jar: /home/mp/data-algorithms-book/dist/data_algorithms_book.jar

BUILD SUCCESSFUL
Total time: 2 seconds
````