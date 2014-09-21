from pyspark import SparkContext
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: SparkSecondarySort [sparkmaster] [file]"
        sys.exit(-1)
    master = sys.argv[1]
    sc = SparkContext(master, "SparkSecondarySort")

    lines = sc.textFile(sys.argv[2])

    pairs = lines.map(lambda x: (x.split(",")[0],(x.split(",")[1],x.split(",")[2])))
    groups = pairs.groupByKey()

    output = groups.mapValues(lambda x: sorted(x)).collect()

    for elem in output:
        print elem
