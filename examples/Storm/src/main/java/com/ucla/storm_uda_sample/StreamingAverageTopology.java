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
import com.ucla.storm_uda_sample.datapoint.AvgDatapoint;
import com.ucla.storm_uda_sample.datapoint.RealDatapoint;
import com.ucla.storm_uda_sample.spout.RealValueSpout;
import com.ucla.streams_uda.core.UdaManager;
import com.ucla.streams_uda.state_storage.MysqlStorageConnectionProvider;
import com.ucla.streams_uda.wrappers.StormUdaBolt;

/**
 * An example of a very basic UDA for streaming average.
 */
public class StreamingAverageTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // Get instance on UDAManager
        UdaManager udaManager = UdaManager.getInstance();
        // Set provider for state storage
        udaManager.setStateStorageConnection(
                new MysqlStorageConnectionProvider("jdbc:mysql://localhost/", "udastate", "root", "linux", "")
        );

        // Declare the AVG UDA
        udaManager.declareUda("AVG", "AVG.txt", RealDatapoint.class, AvgDatapoint.class);
        // Get instance of a Bolt for the AVG UDA
        BaseRichBolt avgBolt = new StormUdaBolt(udaManager.getUdaObject("AVG"));

        // Build the Storm topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomNum", new RealValueSpout(1000), 1);
        builder.setBolt("avg", avgBolt, 1).shuffleGrouping("randomNum");
        builder.setBolt("writeToFile", new WriteToFileBolt("avg_output.txt", "avg"), 1).shuffleGrouping("avg");

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
