package com.ucla.java8streams_uda_sample;

import com.ucla.java8streams_uda_sample.datapoint.BitDatapoint;
import com.ucla.java8streams_uda_sample.datapoint.CountDatapoint;
import com.ucla.java8streams_uda_sample.stream_provider.BitDataStreamProvider;
import com.ucla.streams_uda.core.UdaManager;
import com.ucla.streams_uda.state_storage.MysqlStorageConnectionProvider;
import com.ucla.streams_uda.wrappers.Java8StreamMap;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.stream.Stream;

/**
 * Example of approximate counting UDA using Exponential Histograms [Datar, et.al.].
 */
public class CountSketch {
    public static void main(String[] args) throws IOException {
        // Get instance on UDAManager
        UdaManager udaManager = UdaManager.getInstance();
        // Set provider for state storage
        udaManager.setStateStorageConnection(
                new MysqlStorageConnectionProvider("jdbc:mysql://localhost/", "udastate", "root", "linux", "")
        );

        // Register UDA Definition for approximate counting
        udaManager.declareUda("COUNTN", "COUNTN.txt", BitDatapoint.class, CountDatapoint.class);
        // Get a Java 8 Function implementation for the UDA
        Java8StreamMap<BitDatapoint, Stream<CountDatapoint>> countUda = new Java8StreamMap<>(udaManager.getUdaObject("COUNTN"));

        // PrintWriter to write the output stream to a file
        PrintWriter pw = new PrintWriter(new FileWriter("approx_count.txt"));

        // Create stream, run through UDA, write output stream to file
        new BitDataStreamProvider(10000)
                .getStream()
                .flatMap(countUda)
                .map(c -> c.count)
                .forEach(pw::println);

        // Close PrintWriter
        pw.close();
    }
}
