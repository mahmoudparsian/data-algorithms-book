package org.dataalgorithms.chap01.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** 
 * The DateTemperatureGroupingComparator class
 * enable us to compare two DateTemperaturePair 
 * objects. This class is needed for sorting 
 * purposes.
 *
 * @author Mahmoud Parsian
 *
 */
public class DateTemperatureGroupingComparator 
   extends WritableComparator {

    public DateTemperatureGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }

    @Override
    /**
     * Compare two objects
     * 
     * @param wc1 a WritableComparable object, which represents a DateTemperaturePair
     * @param wc2 a WritableComparable object, which represents a DateTemperaturePair
     * @return 0, 1, or -1 (depending on the comparison of two DateTemperaturePair objects).
     */
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        DateTemperaturePair pair = (DateTemperaturePair) wc1;
        DateTemperaturePair pair2 = (DateTemperaturePair) wc2;
        return pair.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
