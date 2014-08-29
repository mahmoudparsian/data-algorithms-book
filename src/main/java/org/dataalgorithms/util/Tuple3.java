package org.dataalgorithms.util;

import java.io.Serializable;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * This class represents 3 objects as a tuple.
 *
 * @author Mahmoud Parsian
 *
 */
public class Tuple3<T1,T2,T3 > 
    implements Comparable<Tuple3<T1,T2,T3>>, Serializable {
    private final T1    first;
    private final T2    second;
    private final T3    third;
 
    public static <A, B, C> Tuple3<A, B, C> T3(A a, B b, C c) {
      return new Tuple3<A, B, C>(a, b, c);
    }
   
    public Tuple3(T1 first, 
                  T2 second, 
                  T3 third) {
      this.first = first;
      this.second = second;
      this.third = third;
    }
   
    public T1  first() {
      return first;
    }
   
    public T2  second() {
      return second;
    }
  
    public T3 third() {
      return third;
    }

    @Override
    public int hashCode() {
      HashCodeBuilder hcb = new HashCodeBuilder();
      return hcb.append(first).append(second).append(third).toHashCode();
    }

    public boolean equals(Tuple3<T1,T2,T3> other) {
      if (this == other){
        return true;
      }
      if (other == null){
        return false;
      }
      if (getClass() != other.getClass()){
        return false;
      }
      return (first == other.first || (first != null && first.equals(other.first)))
         && (second == other.second || (second != null && second.equals(other.second)))
         && (third == other.third || (third != null && third.equals(other.third)));
    }
  
    @Override
    public int compareTo(Tuple3<T1,T2,T3> other) {
      if (this == other){
        return 0;
      }
      if (other == null){
        return -1;
      }
      if (getClass() != other.getClass()){
        return -1;
      }
      if ( (first == other.first) || 
         (first != null && first.equals(other.first))
         ) {
        return 0;
      }
     return -1;  
   }  
  
   @Override
   public String toString() {
     StringBuilder sb = new StringBuilder("Tuple3[");
     sb.append(first).append(",").append(second).append(",").append(third);
     return sb.append("]").toString();
   }
}
