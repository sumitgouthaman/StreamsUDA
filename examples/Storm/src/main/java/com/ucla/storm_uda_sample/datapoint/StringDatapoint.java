package com.ucla.storm_uda_sample.datapoint;

/**
 * Datapoint for a string value
 */
public class StringDatapoint {
    // The String value
    public String word;
    // Number of trailing zeros in the hash
    private int trailingZeros = -1;

    /**
     * Getter for the word
     *
     * @return the word
     */
    public String getWord() {
        return word;
    }

    /**
     * The hash of the word
     *
     * @return the hash
     */
    public int hash() {
        return word.hashCode();
    }

    /**
     * Number of trailing zeros in the hash of the word
     *
     * @return number of trailing zeros
     */
    public int trailingZerosInHash() {
        if (trailingZeros != -1) {
            return trailingZeros;
        }

        int h = hash();

        if (h == 0) {
            return 32;
        }

        int count = 0;
        while ((h & 1) != 1) {
            count++;
            h >>= 1;
        }

        trailingZeros = count;
        return count;
    }

    /**
     * CTor
     */
    public StringDatapoint() {
    }

    /**
     * CTor
     *
     * @param w the word
     */
    public StringDatapoint(String w) {
        word = w;
    }
}
