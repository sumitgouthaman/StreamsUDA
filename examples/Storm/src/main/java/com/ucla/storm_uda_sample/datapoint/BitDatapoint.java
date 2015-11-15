package com.ucla.storm_uda_sample.datapoint;

/**
 * Datapoint representing a bit value
 */
public class BitDatapoint {
    // The bit value 0/1
    public int bit;

    /**
     * Getter for the bit [needed by Esper]
     *
     * @return
     */
    public int getBit() {
        return bit;
    }

    /**
     * CTor
     */
    public BitDatapoint() {
    }

    /**
     * 1 arg CTor
     *
     * @param b the bit value
     */
    public BitDatapoint(int b) {
        if (b != 0 && b != 1) {
            throw new IllegalArgumentException("Bit has to be 1 or 0");
        }

        bit = b;
    }
}
