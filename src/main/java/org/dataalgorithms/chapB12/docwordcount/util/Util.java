package org.dataalgorithms.chapB12.docwordcount.util;

import scala.Tuple2;
//
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
//
import org.apache.commons.lang3.StringUtils;

/**
 * A basic utility class: some common methods...  *
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {

    public static List<Tuple2<Integer, String>> iterableToList(Iterable<Tuple2<Integer, String>> iterable) {
        List<Tuple2<Integer, String>> list = new ArrayList<Tuple2<Integer, String>>();
        for (Tuple2<Integer, String> item : iterable) {
            list.add(item);
        }
        return list;
    }

    public static List<Tuple2<String, String>> convertToPairOfWordAndDocument(
            final String line,
            final int N) {
        //
        if ((line == null) || (line.length() < N)) {
            return Collections.emptyList();
        }
        //
        String[] tokens = StringUtils.split(line, ":");
        String documentID = tokens[0];
        String wordsAsString = tokens[1];
        String[] words = wordsAsString.split(",");
        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
        for (String word : words) {
            if (word.matches(".*[,.;]$")) {
                // remove the special char from the end
                word = word.substring(0, word.length() - 1);
            }

            if (word.length() < N) {
                continue;
            }

            list.add(new Tuple2<String, String>(word, documentID));
        }
        //
        return list;
    }

}
