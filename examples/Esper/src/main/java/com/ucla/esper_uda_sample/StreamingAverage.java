package com.ucla.esper_uda_sample;

import com.espertech.esper.client.*;
import com.ucla.esper_uda_sample.data_streamer.RealValueStreamer;
import com.ucla.esper_uda_sample.datapoint.AvgDatapoint;
import com.ucla.esper_uda_sample.datapoint.RealDatapoint;
import com.ucla.streams_uda.core.UdaManager;
import com.ucla.streams_uda.state_storage.MysqlStorageConnectionProvider;
import com.ucla.streams_uda.wrappers.EsperUdaListener;

/**
 * An example of a very basic UDA for streaming average.
 */
public class StreamingAverage {
    public static void main(String[] args) {
        // Setting up Esper related boilerplate
        Configuration cepConfig = new Configuration();

        // Register event types
        cepConfig.addEventType("RealDatapoint", RealDatapoint.class.getName());
        cepConfig.addEventType("AvgDatapoint", AvgDatapoint.class.getName());

        // Get Instance of UdaManager
        UdaManager udaManager = UdaManager.getInstance();
        // Set provider for storing state tables
        udaManager.setStateStorageConnection(
                new MysqlStorageConnectionProvider("jdbc:mysql://localhost/", "udastate", "root", "linux", "")
        );

        // Register the AVG UDA definition
        udaManager.declareUda("AVG", "AVG.txt", RealDatapoint.class, AvgDatapoint.class);

        // Esper boilerplate
        EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
        EPRuntime cepRT = cep.getEPRuntime();
        EPAdministrator cepAdm = cep.getEPAdministrator();

        // Create stream of random real values
        EPStatement cepStatement_Input = cepAdm.createEPL("select val from RealDatapoint");

        // Get UDA object for the AVG UDA
        EsperUdaListener avgUda = new EsperUdaListener(udaManager.getUdaObject("AVG"), cepRT);

        // Register the UDA object as a listener
        cepStatement_Input.addListener(avgUda);

        // Get stream of the generated avg values
        EPStatement cepStatement_Count = cepAdm.createEPL("select avg from AvgDatapoint");

        // Register ;istener to print out stream of generated average values
        AvgDatapointListener avgListener = new AvgDatapointListener();
        cepStatement_Count.addListener(avgListener);

        // Start stream of 1000 random real values
        Thread t = new Thread(new RealValueStreamer(cepRT, 1000));
        t.start();

        System.out.println("Started Data Stream");
    }

    /**
     * Listens to incoming average values and print them out
     */
    public static class AvgDatapointListener implements UpdateListener {
        public void update(EventBean[] newData, EventBean[] oldData) {
            for (EventBean e : newData) {
                String avg = e.get("avg").toString();
                System.out.println("Current Avg: " + avg);
            }
        }
    }
}
