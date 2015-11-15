package com.ucla.java8streams_uda_sample.stream_provider;

import com.ucla.java8streams_uda_sample.datapoint.BitDatapoint;

import java.util.stream.Stream;

/**
 * Provides a stream of random bits
 * <p>
 * Frequency of 1s and 0s:
 * - For every sequence of 10000 bits
 * first 2500 bits are produced with the probability of a 0 as 75%
 * next 2500 bits are produced with the probability of a 0 as 25%
 * next 2500 bits are produced with the probability of a 0 as 50%
 * next 2500 bits are produced with the probability of a 0 as 75%
 */
public class BitDataStreamProvider implements StreamProvider {

    // How many bits to stream.
    // If the specified number of bits is streamed before the topology ends, it stops generating more.
    private int howMany;

    /**
     * CTor
     *
     * @param howMany number of bits to stream
     */
    public BitDataStreamProvider(int howMany) {
        this.howMany = howMany;
    }

    /**
     * Override of getStream
     *
     * @return a stream of BitDatapoint objects
     */
    @Override
    public Stream<BitDatapoint> getStream() {
        return Stream.generate(() -> {
            if (howMany-- > 0) {
                double threshold = 0.5; // default threshold

                if (howMany % 10000 < 2500) {
                    threshold = 0.75; // Probability of bit being 0
                } else if (howMany % 10000 < 5000) {
                    threshold = 0.50; // Probability of bit being 0
                } else if (howMany % 10000 < 7500) {
                    threshold = 0.25; // Probability of bit being 0
                } else {
                    threshold = 0.75; // Probability of bit being 0
                }

                BitDatapoint toBeReturned;

                if (Math.random() < threshold) {
                    toBeReturned = new BitDatapoint(0);
                } else {
                    toBeReturned = new BitDatapoint(1);
                }

                if (howMany % 1000 == 0) {
                    System.out.println("To go: " + howMany);
                }
                return toBeReturned;
            }
            return null;
        }).limit(howMany);
    }
}
