package org.dataalgorithms.chap01.spark;

import org.apache.spark.Partitioner;
//
import scala.Tuple2;

/**
 * CustomPartitioner: a custom partitioner for Secondary Sort design pattern
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class CustomPartitioner extends Partitioner {

    private static final long serialVersionUID = -4130944499909424595L;

    private final int numPartitions;

    public CustomPartitioner(int partitions) {
        assert (partitions > 0);
        this.numPartitions = partitions;
    }

    @Override
    public int getPartition(java.lang.Object key) {
        if (key == null) {
            return 0;
        } else if (key instanceof Tuple2) {
            @SuppressWarnings("unchecked")
            Tuple2<String, Integer> tuple2 = (Tuple2<String, Integer>) key;
            return Math.abs(tuple2._1.hashCode() % numPartitions);
        } else {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + numPartitions;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        //
        if (obj == null) {
            return false;
        }
        //
        if (!(obj instanceof CustomPartitioner)) {
            return false;
        }
        //
        CustomPartitioner other = (CustomPartitioner) obj;
        if (numPartitions != other.numPartitions) {
            return false;
        }
        //
        return true;
    }
}
