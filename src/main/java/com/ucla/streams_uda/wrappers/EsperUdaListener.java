package com.ucla.streams_uda.wrappers;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.ucla.streams_uda.core.UdaObject;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper for UdaObject to use with Esper.
 * <p>
 * The object behaves as a UpdateListener in Esper.
 */
public class EsperUdaListener implements UpdateListener {
    // The UdaObject being wrapped
    private UdaObject udaObject;
    // Mappings for names that differ between the input object of the UDA and the esper query result
    private Map<String, String> nameMappings;
    // The Esper runtime
    private EPRuntime epRuntime;
    // Class object representing input object type of the UDA
    private Class inputType;
    // Class object representing output object type of the UDA
    private Class outputType;
    // Map from Field name to Field of the input object
    private Map<String, Field> inputFieldMap;

    /**
     * Ctor
     *
     * @param udaObject The UdaObject being wrapped
     * @param epRuntime The Esper runtime
     */
    public EsperUdaListener(UdaObject udaObject, EPRuntime epRuntime) {
        this(udaObject, epRuntime, null);
    }

    /**
     * CTor
     *
     * @param udaObject    The UDA object being wrapped
     * @param epRuntime    The Esper runtime
     * @param nameMappings The name mappings from input object fields to output properties of Esper query
     */
    public EsperUdaListener(UdaObject udaObject, EPRuntime epRuntime, Map<String, String> nameMappings) {
        this.udaObject = udaObject;
        this.udaObject.inflate();
        this.nameMappings = nameMappings != null ? nameMappings : new HashMap<String, String>();
        this.epRuntime = epRuntime;
        this.inputType = udaObject.getInputType();
        this.outputType = udaObject.getOutputType();

        inputFieldMap = new HashMap<>();
        for (Field field : inputType.getFields()) {
            inputFieldMap.put(field.getName(), field);
        }
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
     * Add a set of name mappings at once.
     *
     * @param nameMappings HashMap of name mappings
     */
    public void addNameMappings(Map<String, String> nameMappings) {
        for (String from : nameMappings.keySet()) {
            this.nameMappings.put(from, nameMappings.get(from));
        }
    }

    /**
     * Process new event
     *
     * @param newEvents Array of new event beans
     * @param oldEvents Array of old event beans (ignored)
     */
    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        for (EventBean e : newEvents) {
            // Get list of properties (fields) in the output of the esper query
            String[] properties = e.getEventType().getPropertyNames();
            try {
                Object inputEvent = inputType.newInstance();
                for (String property : properties) {
                    String propertyVal = e.get(property).toString();

                    // If there is a name mapping for this property, use it instead
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
                for (Object output : outputs) {
                    epRuntime.sendEvent(output);
                }
            } catch (InstantiationException ie) {
                ie.printStackTrace();
            } catch (IllegalAccessException e1) {
                e1.printStackTrace();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Populate a certain field in the given instance of the input object from the value provided as a String.
     *
     * @param targetObject The target object
     * @param targetField  The target field
     * @param propertyVal  The value of the property as a String
     * @throws IllegalAccessException if the field cannot be accessed
     */
    private void fillField(Object targetObject, Field targetField, String propertyVal) throws IllegalAccessException {
        if (targetField.getType() == int.class) {
            int val = Integer.parseInt(propertyVal);
            targetField.setInt(targetObject, val);
            return;
        } else if (targetField.getType() == double.class) {
            double val = Double.parseDouble(propertyVal);
            targetField.setDouble(targetObject, val);
            return;
        } else if (targetField.getType() == String.class) {
            String val = propertyVal;
            targetField.set(targetObject, val);
            return;
        }
    }
}
