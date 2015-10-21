package com.ucla.streams_uda.core;

import java.io.Serializable;

/**
 * Created by sumit on 10/8/15.
 */
public class UdaSpec implements Serializable {
    public String udaName;
    public String fileName;
    public Class inputType;
    public Class outputType;

    public UdaSpec(String udaName, String fileName, Class inputType, Class outputType) {
        this.udaName = udaName;
        this.fileName = fileName;
        this.inputType = inputType;
        this.outputType = outputType;
    }
}
