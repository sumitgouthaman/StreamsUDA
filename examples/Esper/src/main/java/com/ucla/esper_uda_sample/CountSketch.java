package com.ucla.esper_uda_sample;

import com.espertech.esper.client.*;
import com.ucla.esper_uda_sample.data_streamer.BitDataStreamer;
import com.ucla.esper_uda_sample.datapoint.BitDatapoint;
import com.ucla.esper_uda_sample.datapoint.CountDatapoint;
import com.ucla.streams_uda.core.UdaManager;
import com.ucla.streams_uda.state_storage.MysqlStorageConnectionProvider;
import com.ucla.streams_uda.wrappers.EsperUdaListener;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Example of approximate counting UDA using Exponential Histograms [Datar, et.al.].
 */
public class CountSketch {
    public static void main(String[] args) {
        // Esper related boilerplate
        Configuration cepConfig = new Configuration();

        // Register event types
        cepConfig.addEventType("BitDatapoint", BitDatapoint.class.getName());
        cepConfig.addEventType("CountDatapoint", CountDatapoint.class.getName());

        // Esper related boilerplate
        EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
        EPRuntime cepRT = cep.getEPRuntime();
        EPAdministrator cepAdm = cep.getEPAdministrator();

        // Get instance of UdaManager
        UdaManager udaManager = UdaManager.getInstance();
        // Set provider for storing state tables
        udaManager.setStateStorageConnection(
                new MysqlStorageConnectionProvider("jdbc:mysql://localhost/", "udastate", "root", "linux", "")
        );

        // Register UDA Definition for approximate counting
        udaManager.declareUda("COUNTN", "COUNTN.txt", BitDatapoint.class, CountDatapoint.class);

        // Register query for stream of bits
        EPStatement cepStatement_Input = cepAdm.createEPL("select bit from BitDatapoint");

        // Register the UDA as a listener of the stream
        EsperUdaListener countUda = new EsperUdaListener(udaManager.getUdaObject("COUNTN"), cepRT);
        cepStatement_Input.addListener(countUda);

        // Register query for generated approx count values
        EPStatement cepStatement_Count = cepAdm.createEPL("select count from CountDatapoint");

        // Register listener that listens and writes approx count stream to file
        CountValueDatapointListener countListener = new CountValueDatapointListener();
        cepStatement_Count.addListener(countListener);

        // Start stream of bits
        Thread t = new Thread(new BitDataStreamer(cepRT, 10000));
        t.start();

        System.out.println("Started Data Stream");
    }

    /**
     * Listener for generated stream of approx counts. Writes them to file.
     */
    public static class CountValueDatapointListener implements UpdateListener {
        // Printwriter to output file
        private PrintWriter pw;

        /**
         * CTor
         */
        public CountValueDatapointListener() {
            try {
                pw = new PrintWriter(new FileWriter("approx_count.txt"), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Overriding the update method
         *
         * @param newData new events
         * @param oldData old events
         */
        public void update(EventBean[] newData, EventBean[] oldData) {
            for (EventBean e : newData) {
                String count = e.get("count").toString();
                pw.println(count);
            }
        }
    }
}
