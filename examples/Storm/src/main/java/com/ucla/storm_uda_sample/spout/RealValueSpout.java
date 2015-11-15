package com.ucla.storm_uda_sample.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Stream a random sequence of real numbers between 0 (inclusive) and 100 (exclusive)
 */
public class RealValueSpout extends BaseRichSpout {
    // Collector
    private SpoutOutputCollector _collector;
    // How many values to generate
    private int howMany;

    /**
     * CTor
     *
     * @param howMany number of values to generate
     */
    public RealValueSpout(int howMany) {
        this.howMany = howMany;
    }

    /**
     * Override of the declareOutputFields method
     *
     * @param declarer declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("val"));
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
            double val = Math.random() * 100.0;
            _collector.emit(new Values(val));
        }
    }
}
