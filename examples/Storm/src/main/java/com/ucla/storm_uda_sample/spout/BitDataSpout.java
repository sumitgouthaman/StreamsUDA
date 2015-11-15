package com.ucla.storm_uda_sample.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Streams a series of random bits.
 * <p>
 * Frequency of 1s and 0s:
 * - For every sequence of 10000 bits
 * first 2500 bits are produced with the probability of a 0 as 75%
 * next 2500 bits are produced with the probability of a 0 as 25%
 * next 2500 bits are produced with the probability of a 0 as 50%
 * next 2500 bits are produced with the probability of a 0 as 75%
 */
public class BitDataSpout extends BaseRichSpout {
    // Collector
    private SpoutOutputCollector _collector;
    // How many bits to stream.
    // If the specified number of bits is streamed before the topology ends, it stops generating more.
    private int howMany;

    /**
     * CTor
     *
     * @param howMany number of bits to stream
     */
    public BitDataSpout(int howMany) {
        this.howMany = howMany;
    }

    /**
     * Override of the declareOutputFields method
     *
     * @param declarer declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("bit"));
    }

    /**
     * Override of the open method
     *
     * @param conf      conf map
     * @param context   context
     * @param collector collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    /**
     * Override of the nextTuple method
     */
    @Override
    public void nextTuple() {
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

            if (Math.random() < threshold) {
                _collector.emit(new Values(0));
            } else {
                _collector.emit(new Values(1));
            }

            if (howMany % 1000 == 0) {
                System.out.println("To go: " + howMany);
            }
        }
    }
}
