package com.ucla.storm_uda_sample;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.utils.Utils;
import com.ucla.storm_uda_sample.bolt.WriteToFileBolt;
import com.ucla.storm_uda_sample.datapoint.BitDatapoint;
import com.ucla.storm_uda_sample.datapoint.CountDatapoint;
import com.ucla.storm_uda_sample.spout.BitDataSpout;
import com.ucla.streams_uda.core.UdaManager;
import com.ucla.streams_uda.state_storage.MysqlStorageConnectionProvider;
import com.ucla.streams_uda.wrappers.StormUdaBolt;

/**
 * Example of approximate counting UDA using Exponential Histograms [Datar, et.al.].
 */
public class CountSketchTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // Get instance on UDAManager
        UdaManager udaManager = UdaManager.getInstance();
        // Set provider for state storage
        udaManager.setStateStorageConnection(
                new MysqlStorageConnectionProvider("jdbc:mysql://localhost/", "udastate", "root", "linux", "")
        );

        // Register UDA Definition for approximate counting
        udaManager.declareUda("COUNTN", "COUNTN.txt", BitDatapoint.class, CountDatapoint.class);
        // Get instance of a Bolt for the AVG UDA
        BaseRichBolt countBolt = new StormUdaBolt(udaManager.getUdaObject("COUNTN"));

        // Build the Storm topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomBits", new BitDataSpout(10000), 1);
        builder.setBolt("count", countBolt, 1).shuffleGrouping("randomBits");
        builder.setBolt("writeToFile", new WriteToFileBolt("approx_count.txt", "count"), 1).shuffleGrouping("count");

        // Set configuration
        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(1000 * 60);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
