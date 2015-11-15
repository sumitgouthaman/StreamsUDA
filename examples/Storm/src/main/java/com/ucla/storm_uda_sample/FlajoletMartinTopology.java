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
import com.ucla.storm_uda_sample.datapoint.CountDatapoint;
import com.ucla.storm_uda_sample.datapoint.StringDatapoint;
import com.ucla.storm_uda_sample.spout.RandomStringSpout;
import com.ucla.streams_uda.core.UdaManager;
import com.ucla.streams_uda.state_storage.MysqlStorageConnectionProvider;
import com.ucla.streams_uda.wrappers.StormUdaBolt;

/**
 * Example of Flajolet Martin sketch for keeping approximate distinct count in a window using UDA
 */
public class FlajoletMartinTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // Get instance on UDAManager
        UdaManager udaManager = UdaManager.getInstance();
        // Set provider for state storage
        udaManager.setStateStorageConnection(
                new MysqlStorageConnectionProvider("jdbc:mysql://localhost/", "udastate", "root", "linux", "")
        );

        // Register UDA definition for flajolet martin
        udaManager.declareUda("FM", "FLAJOLETMARTIN.txt", StringDatapoint.class, CountDatapoint.class);
        // Get instance of a Bolt for the FM UDA
        BaseRichBolt fmBolt = new StormUdaBolt(udaManager.getUdaObject("FM"));

        // Build the Storm topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomString", new RandomStringSpout(50000, "wordsEn.txt", 5000), 1);
        builder.setBolt("count", fmBolt, 1).shuffleGrouping("randomString");
        builder.setBolt("writeToFile", new WriteToFileBolt("approx_distinct.txt", "count"), 1).shuffleGrouping("count");

        // Set configuration
        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(1000 * 60 * 4);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
