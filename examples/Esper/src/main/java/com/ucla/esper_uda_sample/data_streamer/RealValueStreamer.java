package com.ucla.esper_uda_sample.data_streamer;

import com.espertech.esper.client.EPRuntime;
import com.ucla.esper_uda_sample.datapoint.RealDatapoint;

/**
 * Stream a random sequence of real numbers between 0 (inclusive) and 100 (exclusive)
 */
public class RealValueStreamer implements Runnable {
    // The Esper runtime
    private EPRuntime cepRT;
    // How many values to generate
    private int howMany;

    /**
     * CTor
     *
     * @param cepRT   the Esper runtime
     * @param howMany number of values to Stream
     */
    public RealValueStreamer(EPRuntime cepRT, int howMany) {
        this.cepRT = cepRT;
        this.howMany = howMany;
    }

    /**
     * Override of the run() method
     */
    @Override
    public void run() {
        while (howMany-- > 0) {
            double val = Math.random() * 100.0;
            cepRT.sendEvent(new RealDatapoint(val));
        }
    }
}
