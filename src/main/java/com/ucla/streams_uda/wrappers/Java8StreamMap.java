package com.ucla.streams_uda.wrappers;

import com.ucla.streams_uda.core.UdaObject;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Created by sumit on 10/22/15.
 */
public class Java8StreamMap <T, R extends Stream> implements Function <T,R> {

    private UdaObject udaObject;

    public Java8StreamMap(UdaObject udaObject) {
        this.udaObject = udaObject;
        udaObject.inflate();
    }

    @Override
    public Stream apply(Object t) {
        ArrayList outputs = udaObject.processEvent(t, (Object[]) Array.newInstance(udaObject.getOutputType(), 0));
        return outputs.stream();
    }
}
