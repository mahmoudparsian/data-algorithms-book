package org.dataalgorithms.chap07.spark;

import java.util.List;
import java.util.ArrayList;

/**
 *
 * Some methods for FindAssociationRules class.
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {

    static List<String> toList(String transaction) {
        String[] items = transaction.trim().split(",");
        List<String> list = new ArrayList<String>();
        for (String item : items) {
            list.add(item);
        }
        return list;
    }

    static List<String> removeOneItem(List<String> list, int i) {
        if ((list == null) || (list.isEmpty())) {
            return list;
        }
        //
        if ((i < 0) || (i > (list.size() - 1))) {
            return list;
        }
        //
        List<String> cloned = new ArrayList<String>(list);
        cloned.remove(i);
        //
        return cloned;
    }

}
