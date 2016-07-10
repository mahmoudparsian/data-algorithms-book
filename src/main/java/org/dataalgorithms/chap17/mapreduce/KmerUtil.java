package org.dataalgorithms.chap17.mapreduce;

/**
 * K-mer utility class
 *
 * @author Mahmoud Parsian
 *
 */
public class KmerUtil {

    public static String getKmer(String str, int start, int k) {
        if (start + k > str.length()) {
            return null;
        }
        return str.substring(start, start + k);
    }

    public static void main(String[] args) throws Exception {
        test(args);
        System.exit(0);
    }

    static void test(String[] args) throws Exception {
        final String seq = args[0];  // sequence
        System.out.println("seq=" + seq);
        final int k = Integer.parseInt(args[1]);  // k of k-mer
        System.out.println("k=" + k);
        for (int i = 0; i < seq.length() - k + 1; i++) {
            String kmer = getKmer(seq, i, k);
            System.out.println("kmer=" + kmer);
        }
    }
}
