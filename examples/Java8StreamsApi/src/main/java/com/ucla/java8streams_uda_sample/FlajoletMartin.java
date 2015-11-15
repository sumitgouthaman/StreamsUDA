package com.ucla.java8streams_uda_sample;

import com.ucla.java8streams_uda_sample.datapoint.CountDatapoint;
import com.ucla.java8streams_uda_sample.datapoint.StringDatapoint;
import com.ucla.java8streams_uda_sample.stream_provider.RandomStringStreamProvider;
import com.ucla.streams_uda.core.UdaManager;
import com.ucla.streams_uda.state_storage.MysqlStorageConnectionProvider;
import com.ucla.streams_uda.wrappers.Java8StreamMap;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.stream.Stream;

/**
 * Example of Flajolet Martin sketch for keeping approximate distinct count in a window using UDA
 */
public class FlajoletMartin {
    public static void main(String[] args) throws IOException {
        // Get instance on UDAManager
        UdaManager udaManager = UdaManager.getInstance();
        // Set provider for state storage
        udaManager.setStateStorageConnection(
                new MysqlStorageConnectionProvider("jdbc:mysql://localhost/", "udastate", "root", "linux", "")
        );

        // Register UDA Definition for Flajolet Martin distinct counting
        udaManager.declareUda("FM", "FLAJOLETMARTIN.txt", StringDatapoint.class, CountDatapoint.class);
        // Get a Java 8 Function implementation for the UDA
        Java8StreamMap<StringDatapoint, Stream<CountDatapoint>> fmUda = new Java8StreamMap<>(udaManager.getUdaObject("FM"));

        // PrintWriter to write the output stream to a file
        PrintWriter pw = new PrintWriter(new FileWriter("approx_distinct.txt"));

        // Create stream, run through UDA, write output stream to file
        new RandomStringStreamProvider(50000, "wordsEn.txt", 5000)
                .getStream()
                .flatMap(fmUda)
                .map(c -> c.count)
                .forEach(pw::println);

        // Close PrintWriter
        pw.close();
    }
}
