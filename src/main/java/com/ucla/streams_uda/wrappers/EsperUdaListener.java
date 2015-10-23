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
 * Created by sumit on 10/14/15.
 */
public class EsperUdaListener implements UpdateListener {

    private UdaObject udaObject;
    private Map<String, String> nameMappings;
    private EPRuntime epRuntime;
    private Class inputType;
    private Class outputType;
    private Map<String, Field> inputFieldMap;

    public EsperUdaListener(UdaObject udaObject, EPRuntime epRuntime) {
        this(udaObject, epRuntime, null);
    }

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

    public void addNameMapping(String from, String to) {
        nameMappings.put(from, to);
    }

    public void addNameMappings(Map<String, String> nameMappings) {
        for (String from : nameMappings.keySet()) {
            this.nameMappings.put(from, nameMappings.get(from));
        }
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        for (EventBean e : newEvents) {
            String[] properties = e.getEventType().getPropertyNames();
            try {
                Object inputEvent = inputType.newInstance();
                for (String property : properties) {
                    String propertyVal = e.get(property).toString();
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
