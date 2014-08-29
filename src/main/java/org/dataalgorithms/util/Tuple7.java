package org.dataalgorithms.util;

import java.io.Serializable;

/**
 * This class represents a tuple of 7 objects.
 *
 * @author Mahmoud Parsian
 *
 */
public class Tuple7<T1,T2,T3,T4,T5,T6,T7> implements Serializable {
   final public T1  _1;
   final public T2  _2;
   final public T3  _3;
   final public T4  _4;
   final public T5  _5;
   final public T6  _6;
   final public T7  _7;

   public static <T1,T2,T3,T4,T5,T6,T7> Tuple7<T1,T2,T3,T4,T5,T6,T7> T7(T1 t1,T2 t2,T3 t3,T4 t4,T5 t5,T6 t6,T7 t7) {
      return new Tuple7<T1,T2,T3,T4,T5,T6,T7>(t1,t2,t3,t4,t5,t6,t7);
   }
  
   public Tuple7(T1 t1, 
                 T2 t2, 
                 T3 t3,
                 T4 t4, 
                 T5 t5,
                 T6 t6, 
                 T7 t7) {    
     _1 = t1;
     _2 = t2;
     _3 = t3;
     _4 = t4;     
     _5 = t5;
     _6 = t6;    
     _7 = t7;
   }
}
