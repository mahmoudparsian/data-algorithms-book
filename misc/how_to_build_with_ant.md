Before you build, you should read [README_lib.md](../README_lib.md)

Here I am assuming that your install directory is ````<install-dir-data-algorithms-book>/```` and you are executing build instructions from this directory.

[Apache's ant 1.9.4](http://ant.apache.org/) is used for building the project.

Before you build, you do need to define the following (please edit these directories accordingly):

    # This is an example to set 3 environment variables
    # You should update your script accrodingly
    #
    #set Java as jdk7
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_67.jdk/Contents/Home
    echo "JAVA_HOME=$JAVA_HOME"
    #
    # set ant 
    export ANT_HOME=/Users/mahmoud/apache-ant-1.9.4
    echo "ANT_HOME=$ANT_HOME"
    #
    # set PATH
    export PATH=$JAVA_HOME/bin:$ANT_HOME/bin:$PATH
    echo "PATH=$PATH"


* To clean up:

  **ant clean**

* To build: the build will create ````<install-dir-data-algorithms-book>/dist/data_algorithms_book.jar```` file.

  **ant**

* To check your build environment:

  **ant  myenv**

