package org.dataalgorithms.chapB10.friendrecommendation.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * WritableComparable representing a pair of longs. 
 * The elements in the pair are referred to as the left 
 * and right elements. The natural sort order is: first 
 * by the left element, and then by the right element.
 *
 * @author Mahmoud Parsian
 * Adapted from Jimmy Lin's Cloud9
 *
 */
public class PairOfLongs implements WritableComparable<PairOfLongs> {
   private long left;
   private long right;

   /**
    * Creates a pair of longs.
    */
   public PairOfLongs() {
   }

   /**
    * Creates a pair.
    *
    * @param left the left element
    * @param right the right element
    */
   public PairOfLongs(long left, long right) {
     set(left, right);
   }

   /**
    * Deserializes this pair.
    *
    * @param in source for raw byte representation
    */
   public void readFields(DataInput in) throws IOException {
     left = in.readLong();
     right = in.readLong();
   }

   /**
    * Serializes this pair.
    *
    * @param out where to write the raw byte representation
    */
   public void write(DataOutput out) throws IOException {
     out.writeLong(left);
     out.writeLong(right);
   }

   /**
    * Returns the left.
    *
    * @return the left
    */
   public long getLeft() {
     return left;
   }

   /**
    * Returns the right.
    *
    * @return the right
    */
   public long getRight() {
     return right;
   }


   /**
   * Sets the right and left elements of this pair.
   *
   * @param left the left
   * @param right the right
   */
   public void set(long left, long right) {
     this.left = left;
     this.right = right;
   }

   /**
    * Checks two pairs for equality.
    *
    * @param obj object for comparison
    * @return <code>true</code> if <code>obj</code> is equal to this object, <code>false</code>
    *         otherwise
    */
   public boolean equals(Object obj) {
     PairOfLongs pair = (PairOfLongs) obj;
     return left == pair.getLeft() && right == pair.getRight();
   }

  /**
   * Defines a natural sort order for pairs. Pairs are sorted first by the left element, 
   * and then by the right element.
   *
   * @return a value less than zero, a value greater than zero, or zero if this pair should be
   *         sorted before, sorted after, or is equal to <code>obj</code>.
   */
   public int compareTo(PairOfLongs pair) {
      long L = pair.getLeft();
      long R = pair.getRight();

      if (left == L) {
         if (right < R) {
           return -1;
         }
         if (right > R) {
           return 1;
         }
         return 0;
      }

      if (left < L) {
         return -1;
      }

      return 1;
   }

   /**
    * Returns a hash code value for the pair.
    *
    * @return hash code for the pair
    */
   public int hashCode() {
     return (int) left & (int) right;
   }

   /**
    * Generates human-readable String representation of this pair.
    *
    * @return human-readable String representation of this pair
    */
   public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("(");
      builder.append(left);
      builder.append(", ");
      builder.append(right);
      builder.append(")");
      return builder.toString();
  }

   /**
    * Clones this object.
    *
    * @return clone of this object
    */
   public PairOfLongs clone() {
     return new PairOfLongs(this.left, this.right);
   }

   /** Comparator optimized for <code>PairOfLongs</code>. */
   public static class Comparator extends WritableComparator {

     /**
      * Creates a new Comparator optimized for <code>PairOfLongs</code>.
      */
     public Comparator() {
       super(PairOfLongs.class);
     }

     /**
      * Optimization hook.
      */
     public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
         long thisLeftValue = readLong(b1, s1);
         long thatLeftValue = readLong(b2, s2);

         if (thisLeftValue == thatLeftValue) {
           long thisRightValue = readLong(b1, s1 + 8);
           long thatRightValue = readLong(b2, s2 + 8);
           return (thisRightValue < thatRightValue ? -1 : (thisRightValue == thatRightValue ? 0 : 1));
         }

         return (thisLeftValue < thatLeftValue ? -1 : (thisLeftValue == thatLeftValue ? 0 : 1));
      }
   }

   static { // register this comparator
      WritableComparator.define(PairOfLongs.class, new Comparator());
   }
}
