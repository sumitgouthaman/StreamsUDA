package com.ucla.streams_uda.wrappers;

import com.ucla.streams_uda.core.UdaObject;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Wrapper for the UdaObject for use with Java-8 Streams API.
 * <p>
 * The class acts as a Function object that can be passed as parameter to flatMap()
 */
public class Java8StreamMap<T, R extends Stream> implements Function<T, R> {

    // The UdaObject to be Wrapped
    private UdaObject udaObject;

    /**
     * CTor
     *
     * @param udaObject The UdaObject to be wrapped
     */
    public Java8StreamMap(UdaObject udaObject) {
        this.udaObject = udaObject;
        udaObject.inflate();
    }

    /**
     * Apply method implementation
     *
     * @param t The new input object to be processed
     * @return A stream of the output objects
     */
    @Override
    public Stream apply(Object t) {
        ArrayList outputs = udaObject.processEvent(t, (Object[]) Array.newInstance(udaObject.getOutputType(), 0));
        return outputs.stream();
    }
}
