package com.ucla.streams_uda.wrappers;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.ucla.streams_uda.core.UdaObject;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper for the UdaObject that acts as a Storm Bolt
 */
public class StormUdaBolt extends BaseRichBolt {
    // Storm collector object
    OutputCollector _collector;
    // The UdaObject being wrapped
    private UdaObject udaObject;
    // Mappings for cases when incoming tuple fields of the bolt don't match UDA input object fields
    private Map<String, String> nameMappings;
    // Class object representing input object type
    private Class inputType;
    // Class ibject representing output object type
    private Class outputType;
    // Map of Field name to input Field object for quicker access
    private Map<String, Field> inputFieldMap;

    /**
     * CTor
     *
     * @param udaObject The UdaObject to be wrapped
     */
    public StormUdaBolt(UdaObject udaObject) {
        this(udaObject, null);
    }

    /**
     * CTor
     *
     * @param udaObject    The UdaObject to be wrapped
     * @param nameMappings HashMap of name mappings
     */
    public StormUdaBolt(UdaObject udaObject, Map<String, String> nameMappings) {
        this.udaObject = udaObject;
        this.nameMappings = nameMappings != null ? nameMappings : new HashMap<String, String>();
        this.inputType = udaObject.getInputType();
        this.outputType = udaObject.getOutputType();
    }

    /**
     * Add a single name mapping
     *
     * @param from from name
     * @param to   to name
     */
    public void addNameMapping(String from, String to) {
        nameMappings.put(from, to);
    }

    /**
     * Add a set of mappings at once
     *
     * @param nameMappings HashMap of name mappings
     */
    public void addNameMappings(Map<String, String> nameMappings) {
        for (String from : nameMappings.keySet()) {
            this.nameMappings.put(from, nameMappings.get(from));
        }
    }

    /**
     * Override of the prepare method
     *
     * @param stormConf Storm config
     * @param context   Context
     * @param collector Collector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        this.udaObject.inflate();
        inputFieldMap = new HashMap<>();
        for (Field field : inputType.getFields()) {
            inputFieldMap.put(field.getName(), field);
        }
    }

    /**
     * Override of the execute method
     *
     * @param input Input tuple
     */
    @Override
    public void execute(Tuple input) {
        try {
            Object inputEvent = inputType.newInstance();
            for (String property : input.getFields()) {
                String propertyVal = input.getValueByField(property).toString();
                String targetFieldName = nameMappings.containsKey(property) ?
                        nameMappings.get(property) : property;
                if (!inputFieldMap.containsKey(targetFieldName)) {
                    throw new RuntimeException(String.format("Target field name %s not found in input object",
                            targetFieldName));
                }
                Field targetField = inputFieldMap.get(targetFieldName);
                fillField(inputEvent, targetField, propertyVal);
            }

            ArrayList outputs = udaObject.processEvent(inputEvent, (Object[]) Array.newInstance(outputType, 0));

            // Form a tuple object for the output
            for (Object output : outputs) {
                Values values = new Values();
                for (Field field : outputType.getFields()) {
                    try {
                        extractFieldValue(output, field, values);
                    } catch (IllegalAccessException iae) {
                        iae.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                // Emit the output tuple
                _collector.emit(input, values);
            }
            _collector.ack(input);
        } catch (InstantiationException ie) {
            ie.printStackTrace();
        } catch (IllegalAccessException e1) {
            e1.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Get specific field value from the object and insert into Values instance
     *
     * @param output The object to extract the field from
     * @param field  The field to extract
     * @param values The Values instance
     * @throws IllegalAccessException
     */
    private void extractFieldValue(Object output, Field field, Values values) throws IllegalAccessException {
        if (field.getType() == int.class) {
            int val = field.getInt(output);
            values.add(val);
        } else if (field.getType() == double.class) {
            double val = field.getDouble(output);
            values.add(val);
        } else if (field.getType() == String.class) {
            String val = field.get(output).toString();
            values.add(val);
        }
    }

    /**
     * Declare output fields for the Bolt
     *
     * @param declarer Declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        ArrayList<String> fieldNames = new ArrayList<>();
        for (Field field : outputType.getFields()) {
            fieldNames.add(field.getName());
        }
        declarer.declare(new Fields(fieldNames));
    }

    /**
     * Override of the getComponentConfiguration() method
     *
     * @return Map of the configuration
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        return ret;
    }

    /**
     * Fill a specific field of the object when the field values is provided as a String
     *
     * @param targetObject The object to be filled
     * @param targetField  The filed being filled
     * @param propertyVal  The value as a String
     * @throws IllegalAccessException if the field is not accessible
     */
    private void fillField(Object targetObject, Field targetField, String propertyVal) throws IllegalAccessException {
        if (targetField.getType() == int.class) {
            int val = Integer.parseInt(propertyVal);
            targetField.setInt(targetObject, val);
        } else if (targetField.getType() == double.class) {
            double val = Double.parseDouble(propertyVal);
            targetField.setDouble(targetObject, val);
        } else if (targetField.getType() == String.class) {
            String val = propertyVal;
            targetField.set(targetObject, val);
        }
    }
}
