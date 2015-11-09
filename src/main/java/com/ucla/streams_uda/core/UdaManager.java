package com.ucla.streams_uda.core;

import com.ucla.streams_uda.state_storage.IStateStorageConnectionProvider;

import java.util.HashMap;

/**
 * Central class to handle defined UDAs in the System.
 */
public class UdaManager {

    // Singleton instance
    private static UdaManager instance = null;
    // Provides a SQL connection object for the state table
    private IStateStorageConnectionProvider stateStorageConnectionProvider = null;
    // Map of UDA definitions
    private HashMap<String, UdaSpec> declaredUdas = new HashMap<>();

    protected UdaManager() {
        // Singleton class
    }

    /**
     * Get instance of the UdaManager class
     *
     * @return valid instance of UdaManager
     */
    public static UdaManager getInstance() {
        if (instance == null) {
            instance = new UdaManager();
        }

        return instance;
    }

    /**
     * Set provider that will be used for storing state in the UDA
     *
     * @param storageConnectionProvider Valid implementation of IStateStorageConnectionProvider
     */
    public void setStateStorageConnection(IStateStorageConnectionProvider storageConnectionProvider) {
        this.stateStorageConnectionProvider = storageConnectionProvider;
    }

    /**
     * Declare a new UDA
     *
     * @param udaName    name of the UDA
     * @param fileName   file with UDA definition
     * @param inputType  type of input data
     * @param outputType type of output data
     */
    public void declareUda(String udaName, String fileName, Class inputType, Class outputType) {
        UdaSpec udaSpec = new UdaSpec(udaName.trim(), fileName.trim(), inputType, outputType);
        if (declaredUdas.containsKey(udaName.trim())) {
            throw new IllegalArgumentException(String.format("UDA with name %s already defined.", udaName));
        }

        declaredUdas.put(udaName.trim(), udaSpec);
    }

    /**
     * Get implementation of a UDA defined in the system
     *
     * @param udaName name of the reuested UDA
     * @return UdaObject implementation for the UDA
     */
    public UdaObject getUdaObject(String udaName) {
        if (!declaredUdas.containsKey(udaName.trim())) {
            throw new IllegalArgumentException(String.format("UDA with name %s not declared", udaName.trim()));
        }

        UdaSpec udaSpec = declaredUdas.get(udaName.trim());
        UdaObject udaObject = new UdaObject(udaSpec, stateStorageConnectionProvider);
        return udaObject;
    }
}