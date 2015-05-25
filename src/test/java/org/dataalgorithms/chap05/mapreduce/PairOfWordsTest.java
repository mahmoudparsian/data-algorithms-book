package org.dataalgorithms.chap05.mapreduce;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import org.junit.Test;

/**
 * This is a test class. This class tests the map() for Relative Frequency of
 * words.
 *
 * @author Mahmoud Parsian
 *
 */
public class PairOfWordsTest {

    private static final int neighborWindow = 2;
    private static final PairOfWords pair = new PairOfWords();

    @Test
    public void canGetRelativeFrequency() {
        int totalCount;
        String value = "w1 w2 w3 w4 w5 w6";
        String[] tokens = value.split(" ");
        if ((tokens == null) || (tokens.length < 2)) {
            return;
        }

        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].replaceAll("\\W+", "");

            if (tokens[i].equals("")) {
                continue;
            }

            pair.setWord(tokens[i]);

            int start = (i - neighborWindow < 0) ? 0 : i - neighborWindow;
            int end = (i + neighborWindow >= tokens.length) ? tokens.length - 1 : i + neighborWindow;
            for (int j = start; j <= end; j++) {
                if (j == i) {
                    continue;
                }
                pair.setNeighbor(tokens[j].replaceAll("\\W", ""));

                System.out.println("pair=" + pair + " 1");
            }
            pair.setNeighbor("*");
            totalCount = end - start;
            System.out.println("pair=" + pair + "  " + totalCount);
            assertThat(totalCount, greaterThan(1));
        }
    }
}

/*
 Sample run:

 # java org.dataalgorithms.chap05.TestMapper
 pair=(w1, w2) 1
 pair=(w1, w3) 1
 pair=(w1, *)  2
 pair=(w2, w1) 1
 pair=(w2, w3) 1
 pair=(w2, w4) 1
 pair=(w2, *)  3
 pair=(w3, w1) 1
 pair=(w3, w2) 1
 pair=(w3, w4) 1
 pair=(w3, w5) 1
 pair=(w3, *)  4
 pair=(w4, w2) 1
 pair=(w4, w3) 1
 pair=(w4, w5) 1
 pair=(w4, w6) 1
 pair=(w4, *)  4
 pair=(w5, w3) 1
 pair=(w5, w4) 1
 pair=(w5, w6) 1
 pair=(w5, *)  3
 pair=(w6, w4) 1
 pair=(w6, w5) 1
 pair=(w6, *)  2
 */
