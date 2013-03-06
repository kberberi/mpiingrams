package de.mpii.ngrams.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Collection of functions from string processing (e.g., prefix/suffix check and
 * longest common prefix) implemented over int[] and primitive collections.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class StringUtils {

    /**
     * Returns true if <i>a</i> is a prefix of <i>s</i>.
     */
    public static boolean isPrefix(final IntArrayList a, int[] s) {
        if (a == null || s == null || a.size() > s.length) {
            return false;
        }

        for (int i = 0; i < a.size(); i++) {
            if (a.get(i) != s[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns length of longest common prefix between <i>a</i> and
     * <i>b</i>.
     */
    public static int lcp(int[] a, int[] b) {
        int lcp = 0;
        while (lcp < a.length && lcp < b.length && a[lcp] == b[lcp]) {
            lcp++;
        }
        return lcp;
    }

    /**
     * Returns length of longest common prefix between <i>a</i> and <i>b</i>.
     */
    public static int lcp(IntArrayList a, int[] b) {
        int lcp = 0;
        int aSize = a.size();
        while (lcp < aSize && lcp < b.length && a.get(lcp) == b[lcp]) {
            lcp++;
        }
        return lcp;
    }

    /**
     * Returns true if <i>a</i> is a prefix of <i>s</i>.
     */
    public static boolean isPrefix(int[] a, int[] s) {
        if (a == null || s == null || a.length > s.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != s[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns true if <i>a</i> is a prefix of <i>s</i>.
     */
    public static boolean isSuffix(int[] a, int[] s) {
        if (a == null || s == null || a.length > s.length) {
            return false;
        }

        for (int i = 1; i <= a.length; i++) {
            if (a[a.length - i] != s[s.length - i]) {
                return false;
            }
        }

        return true;
    }
}
