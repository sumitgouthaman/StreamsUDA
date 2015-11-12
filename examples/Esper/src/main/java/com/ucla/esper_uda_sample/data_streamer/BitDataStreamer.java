package com.ucla.esper_uda_sample.data_streamer;

import com.espertech.esper.client.EPRuntime;
import com.ucla.esper_uda_sample.datapoint.BitDatapoint;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

/**
 * Streams a series of random bits.
 * <p>
 * Frequency of 1s and 0s:
 * - For every sequence of 10000 bits
 * first 2500 bits are produced with the probability of a 0 as 75%
 * next 2500 bits are produced with the probability of a 0 as 50%
 * next 2500 bits are produced with the probability of a 0 as 25%
 * next 2500 bits are produced with the probability of a 0 as 75%
 */
public class BitDataStreamer implements Runnable {
    // The Esper runtime
    private EPRuntime cepRT;
    // How many values to produce
    private int howMany;
    // Printwriter to write the bits generated to a file
    private PrintWriter inputPw;
    // Printwriter that writes the actual count to a file [used for comparing with the approx count]
    private PrintWriter countPw;
    // The window size
    private final int windowSize = 1000;
    // The actual count in the window
    private int count = 0;
    // The list of actual bits in the window [so that actual count can be generated for comparison]
    private LinkedList<Integer> list = new LinkedList<>();

    /**
     * CTor
     *
     * @param cepRT   the Esper runtime
     * @param howMany how many bits to produce
     */
    public BitDataStreamer(EPRuntime cepRT, int howMany) {
        this.cepRT = cepRT;
        this.howMany = howMany;
        count = 0;
        try {
            inputPw = new PrintWriter(new FileWriter("input.txt"));
            countPw = new PrintWriter(new FileWriter("actual_count.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Overriding the run() method
     */
    @Override
    public void run() {
        while (howMany-- > 0) {
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

            if (Math.random() < threshold) {
                cepRT.sendEvent(new BitDatapoint(0));
                list.add(0);
                inputPw.println(0);
            } else {
                cepRT.sendEvent(new BitDatapoint(1));
                count++;
                list.add(1);
                inputPw.println(1);
            }

            // Maintain the window count
            if (list.size() > 1000) {
                int outGoing = list.poll();
                if (outGoing == 1) {
                    count--;
                }
            }

            countPw.println(count);

            if (howMany % 1000 == 0) {
                System.out.println("To go: " + howMany);
            }
        }

        inputPw.close();
        countPw.close();
    }
}
