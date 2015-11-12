package com.ucla.esper_uda_sample;

import com.espertech.esper.client.*;
import com.ucla.esper_uda_sample.data_streamer.RandomStringStreamer;
import com.ucla.esper_uda_sample.datapoint.CountDatapoint;
import com.ucla.esper_uda_sample.datapoint.StringDatapoint;
import com.ucla.streams_uda.core.UdaManager;
import com.ucla.streams_uda.state_storage.MysqlStorageConnectionProvider;
import com.ucla.streams_uda.wrappers.EsperUdaListener;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Example of Flajolet Martin sketch for keeping approximate distinct count in a window using UDA
 */
public class FlajoletMartinSketch {
    public static void main(String[] args) {
        // Esper Boilerplate
        Configuration cepConfig = new Configuration();

        // Register events
        cepConfig.addEventType("StringDatapoint", StringDatapoint.class.getName());
        cepConfig.addEventType("CountDatapoint", CountDatapoint.class.getName());

        // Esper Boilerplate
        EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
        EPRuntime cepRT = cep.getEPRuntime();
        EPAdministrator cepAdm = cep.getEPAdministrator();

        // Get instance of UdaManager
        UdaManager udaManager = UdaManager.getInstance();
        // Register provider for storing state table
        udaManager.setStateStorageConnection(
                new MysqlStorageConnectionProvider("jdbc:mysql://localhost/", "udastate", "root", "linux", "")
        );

        // Register UDA definition for flajolet martin
        udaManager.declareUda("FM", "FLAJOLETMARTIN.txt", StringDatapoint.class, CountDatapoint.class);

        // Register query for stream of random words
        EPStatement cepStatement_Input = cepAdm.createEPL("select word from StringDatapoint");

        // Get UdaObject for the FM UDA and register as a listener
        EsperUdaListener countUda = new EsperUdaListener(udaManager.getUdaObject("FM"), cepRT);
        cepStatement_Input.addListener(countUda);

        // Register query for stream of generated distinct count
        EPStatement cepStatement_Count = cepAdm.createEPL("select count from CountDatapoint");

        // Register listener to write the distinct count stream to file
        CountValueDatapointListener countListener = new CountValueDatapointListener();
        cepStatement_Count.addListener(countListener);

        // Start stream of random words
        Thread t = new Thread(new RandomStringStreamer(cepRT, 50000, "wordsEn.txt", 5000));
        t.start();

        System.out.println("Started Data Stream");
    }

    /**
     * Listener to listen to stream of generated distinct counts for the current window.
     * Stores it into a text file.
     */
    public static class CountValueDatapointListener implements UpdateListener {
        // PrintWriter to write to file
        private PrintWriter pw;

        /**
         * CTor
         */
        public CountValueDatapointListener() {
            try {
                pw = new PrintWriter(new FileWriter("approx_distinct.txt"), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Override of the update() method
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
