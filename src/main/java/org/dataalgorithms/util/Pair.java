package org.dataalgorithms.util;

import java.util.Scanner;
import org.apache.log4j.Logger;

/**
 * Container for two Java objects (similar to CAR and CDR in Lisp!). 
 * This class is used for handling paired/unpaired reads of genome raw data.
 *
 * @author Mahmoud Parsian
 *
 */
public class Pair<TYPE1, TYPE2> implements Comparable {

    private static final Logger THE_LOGGER = Logger.getLogger(Pair.class);

    private TYPE1 t1 = null;
    private TYPE2 t2 = null;

    public Pair(TYPE1 t1, TYPE2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    /**
     * Returns the left item of the pair.
     *
     * @return the left item
     */
    public TYPE1 getLeft() {
        return this.t1;
    }

    /**
     * Returns the right item of the pair.
     *
     * @return the right item
     */
    public TYPE2 getRight() {
        return this.t2;
    }

    /**
     * set left item
     *
     */
    public void setLeft(TYPE1 t1) {
        this.t1 = t1;
    }

    /**
     * set right item
     *
     */
    public void setRight(TYPE2 t2) {
        this.t2 = t2;
    }

    @Override
    public int hashCode() {
        int code = 0;
        if (t1 != null) {
            code = t1.hashCode();
        }
        if (t2 != null) {
            code = code / 2 + t2.hashCode() / 2;
        }
        return code;
    }

    public static boolean same(Object t1, Object t2) {
        return t1 == null ? t2 == null : t1.equals(t2);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair)) {
            return false;
        }
        Pair p = (Pair) obj;
        return same(p.t1, this.t1) && same(p.t2, this.t2);
    }

    public int compareTo(Object obj) {
        if (!(obj instanceof Pair)) {
            return +1;
        }
        Pair p = (Pair) obj;
        if (same(p.t1, this.t1) && same(p.t2, this.t2)) {
            return 0;
        }

        return -1;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Pair{");
        builder.append(t1);
        builder.append(", ");
        builder.append(t2);
        builder.append("}");
        return builder.toString();
    }

    public static String join(Pair<String, String> pair, String delimiter) {
        if (pair == null) {
            return null;
        }
        return pair.getLeft() + delimiter + pair.getRight();
    }

    public static Pair<String, String> getPairOfString(String line, String delimiter)
            throws Exception {
        Scanner scanner = null;
        Pair<String, String> pair = null;
        try {
            scanner = new Scanner(line);
            scanner.useDelimiter(delimiter); // two strings are separated by a single delimiter such as ";"
            String str1 = scanner.next();
            THE_LOGGER.info("getPairOfString(): str1=" + str1);
            String str2 = scanner.next();
            THE_LOGGER.info("getPairOfString(): str2=" + str2);
            pair = new Pair<String, String>(str1, str2);
        } 
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return pair;
    }

    /**
     * Simple test program.
     */
    public static void main(String[] args) {
        Pair<String, String> p1 = new Pair<String, String>("a1", "b1");
        Pair<String, String> p2 = new Pair<String, String>("a1", null);
        Pair<String, String> p3 = new Pair<String, String>("a1", "b1");
        Pair<String, String> p4 = new Pair<String, String>(null, null);

        THE_LOGGER.info("p1=" + p1);
        THE_LOGGER.info("p2=" + p2);
        THE_LOGGER.info("p3=" + p3);
        THE_LOGGER.info("p4=" + p4);

        THE_LOGGER.info(p1.equals(new Pair<Integer, Integer>(1, 2)) + " should be false");
        THE_LOGGER.info(p4.equals(p2) + " should be false");
        THE_LOGGER.info(p2.equals(p4) + " should be false");
        THE_LOGGER.info(p1.equals(p3) + " should be true");
        THE_LOGGER.info(p4.equals(p4) + " should be true");
    }

}
