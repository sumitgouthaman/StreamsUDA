package com.ucla.streams_uda.core;

import java.io.Serializable;

/**
 * Class that wraps the specs of a defined UDA
 */
public class UdaSpec implements Serializable {
    // Unique name for the UDA
    public String udaName;
    // Filename of the UDA definition
    public String fileName;
    // Class object representing input object type
    public Class inputType;
    // Class object representing output object type
    public Class outputType;

    /**
     * CTor
     *
     * @param udaName    Name of the UDA
     * @param fileName   Filename of the UDA definition
     * @param inputType  Class object representing input object type
     * @param outputType Class object representing output object type
     */
    public UdaSpec(String udaName, String fileName, Class inputType, Class outputType) {
        this.udaName = udaName;
        this.fileName = fileName;
        this.inputType = inputType;
        this.outputType = outputType;
    }
}
