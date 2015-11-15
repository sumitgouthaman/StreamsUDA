package com.ucla.java8streams_uda_sample.stream_provider;

import com.ucla.java8streams_uda_sample.datapoint.RealDatapoint;

import java.util.stream.Stream;

/**
 * Stream a random sequence of real numbers between 0 (inclusive) and 100 (exclusive)
 */
public class RealValueStreamProvider implements StreamProvider {
    // How many values to generate
    private int howMany;

    /**
     * CTor
     *
     * @param howMany number of values to generate
     */
    public RealValueStreamProvider(int howMany) {
        this.howMany = howMany;
    }

    /**
     * Override of getStream
     *
     * @return a stream of RealDatapoint objects
     */
    @Override
    public Stream<RealDatapoint> getStream() {
        return Stream.generate(() -> {
            if (howMany-- > 0) {
                double val = Math.random() * 100.0;
                return new RealDatapoint(val);
            }

            return null;
        }).limit(howMany);
    }
}
