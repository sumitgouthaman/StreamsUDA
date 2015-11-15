package com.ucla.java8streams_uda_sample;

import com.ucla.java8streams_uda_sample.datapoint.AvgDatapoint;
import com.ucla.java8streams_uda_sample.datapoint.RealDatapoint;
import com.ucla.java8streams_uda_sample.stream_provider.RealValueStreamProvider;
import com.ucla.streams_uda.core.UdaManager;
import com.ucla.streams_uda.state_storage.MysqlStorageConnectionProvider;
import com.ucla.streams_uda.wrappers.Java8StreamMap;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.stream.Stream;

/**
 * An example of a very basic UDA for streaming average.
 */
public class StreamingAverage {
    public static void main(String[] args) throws IOException {
        // Get instance on UDAManager
        UdaManager udaManager = UdaManager.getInstance();
        // Set provider for state storage
        udaManager.setStateStorageConnection(
                new MysqlStorageConnectionProvider("jdbc:mysql://localhost/", "udastate", "root", "linux", "")
        );

        // Declare the AVG UDA
        udaManager.declareUda("AVG", "AVG.txt", RealDatapoint.class, AvgDatapoint.class);

        // Get a Java 8 Function implementation for the UDA
        Java8StreamMap<RealDatapoint, Stream<AvgDatapoint>> avgUda = new Java8StreamMap<>(udaManager.getUdaObject("AVG"));

        // PrintWriter to write the output stream to a file
        PrintWriter pw = new PrintWriter(new FileWriter("avg_output.txt"));

        // Create stream, run through UDA, write output stream to file
        new RealValueStreamProvider(1000)
                .getStream()
                .flatMap(avgUda)
                .map(a -> a.avg)
                .forEach(pw::println);

        // Close PrintWriter
        pw.close();
    }
}
