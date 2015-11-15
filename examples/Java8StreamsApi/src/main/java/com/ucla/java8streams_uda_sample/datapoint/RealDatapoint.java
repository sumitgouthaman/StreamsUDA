package com.ucla.java8streams_uda_sample.datapoint;

/**
 * Datapoint for a real value
 */
public class RealDatapoint {
    // The real value
    public double val;

    /**
     * Getter for the real value
     *
     * @return the real value
     */
    public double getVal() {
        return val;
    }

    /**
     * CTor
     */
    public RealDatapoint() {
    }

    /**
     * CTor
     *
     * @param val the real value
     */
    public RealDatapoint(double val) {
        this.val = val;
    }
}
