package org.dataalgorithms.util;

import java.io.Serializable;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * This class represents 3 objects as a tuple.
 *
 * @author Mahmoud Parsian
 *
 */
public class Tuple3<T1,T2,T3 > implements Comparable<Tuple3<T1,T2,T3>>, Serializable {
  public final T1  _1;
  public final T2  _2;
  public final T3  _3;

  public static <A, B, C> Tuple3<A, B, C> T3(A a, B b, C c) {
    return new Tuple3<A, B, C>(a, b, c);
  }
  
  public Tuple3(T1 first,
                T2 second,
                T3 third) {
    this._1 = first;
    this._2 = second;
    this._3 = third;
  }
  
  public T1  first() {
    return _1;
  }
  
  public T2  second() {
    return _2;
  }
  
  public T3 third() {
    return _3;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(_1).append(_2).append(_3).toHashCode();
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
    return (_1 == other._1 || (_1 != null && _1.equals(other._1)))
        && (_2 == other._2 || (_2 != null && _2.equals(other._2)))
        && (_3 == other._3 || (_3 != null && _3.equals(other._3)));
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
    if ( (_1 == other._1) ||
         (_1 != null && _1.equals(other._1))
       ) {
       return 0;
    }
    return -1;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Tuple3[");
    sb.append(_1).append(",").append(_2).append(",").append(_3);
    return sb.append("]").toString();
  }
}