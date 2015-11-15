package com.ucla.java8streams_uda_sample.stream_provider;

import java.util.stream.Stream;

/**
 * Interface StreamProvider
 */
public interface StreamProvider {
    /**
     * Provide a stream implmentation
     *
     * @return a java 8 stream
     */
    public Stream getStream();
}