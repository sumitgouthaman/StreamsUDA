package com.ucla.storm_uda_sample.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 * Bolt that writes a particular field of incoming tuples to a file.
 */
public class WriteToFileBolt extends BaseRichBolt {
    // Collector
    private OutputCollector _collector;
    // PrintWriter to write to file
    private PrintWriter pw = null;
    // File name
    private String fileName;
    // Field to write
    private String fieldName;

    /**
     * CTor
     *
     * @param fileName  output file name
     * @param fieldName field to write
     */
    public WriteToFileBolt(String fileName, String fieldName) {
        this.fieldName = fieldName;
        this.fileName = fileName;
    }

    /**
     * Override of the prepare method
     *
     * @param stormConf conf
     * @param context   context
     * @param collector collector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        try {
            pw = new PrintWriter(new FileWriter(fileName), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Override of the execute method
     *
     * @param input input tuple
     */
    @Override
    public void execute(Tuple input) {
        String val = input.getValueByField(fieldName).toString();
        pw.println(val);
    }

    /**
     * Override of the declareOutputFields method
     *
     * @param declarer declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }

    /**
     * Override of the cleanup method
     */
    @Override
    public void cleanup() {
        super.cleanup();
        pw.close();
    }
}
