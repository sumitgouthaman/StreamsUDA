package com.ucla.streams_uda.core;

import com.ucla.streams_uda.state_storage.IStateStorageConnectionProvider;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Implementation for a specific UDA
 */
public class UdaObject implements Serializable{
    private UdaSpec spec;
    private Connection stateStorageConnection;
    private IStateStorageConnectionProvider stateStorageConnectionProvider;
    private boolean inflated = false;

    private ArrayList<UdaStatement> stateDeclarationStatements = new ArrayList<>();
    private ArrayList<UdaStatement> initializeStatements = new ArrayList<>();
    private ArrayList<UdaStatement> iterateStatements = new ArrayList<>();
    private ArrayList<UdaStatement> terminateStatements = new ArrayList<>();

    private boolean initializeDone = false;

    private Field[] inputFields;
    private Field[] outputFields;
    private Map<String, Field> inputFieldMap;
    private Map<String, Field> outputFieldMap;

    public UdaObject(UdaSpec udaSpec, IStateStorageConnectionProvider stateStorageConnectionProvider) {
        this.spec = udaSpec;
        this.stateStorageConnectionProvider = stateStorageConnectionProvider;
    }

    public void inflate() {
        inflated = true;
        stateStorageConnection = stateStorageConnectionProvider.getConnection();
        inputFields = spec.inputType.getFields();
        outputFields = spec.outputType.getFields();
        inputFieldMap = new HashMap<>();
        outputFieldMap = new HashMap<>();
        for (Field inputField : inputFields) {
            inputFieldMap.put(inputField.getName(), inputField);
        }
        for (Field outputField : outputFields) {
            outputFieldMap.put(outputField.getName(), outputField);
        }

        Scanner sc = null;
        try {
            sc = new Scanner(new FileReader(spec.fileName));
        } catch (FileNotFoundException e) {
            System.err.printf("Could not find file %s for aggregate %s%n", spec.fileName, spec.udaName);
            e.printStackTrace();
            return;
        }

        ParserState parserState = ParserState.STATE_DECLARATION;
        String incompleteQuery = "";
        while (sc.hasNextLine()) {
            String newLine = sc.nextLine().trim();
            if (newLine.matches("(?i)table.*") && parserState == ParserState.STATE_DECLARATION) {
                String query = "CREATE " + newLine;
                UdaStatement stateDeclarationStatement = new UdaStatement(
                        StatementType.STATE_DECLARATION,
                        query);
                stateDeclarationStatements.add(stateDeclarationStatement);
                continue;
            }
            if (newLine.matches("(?i)initialize:\\s*\\{")) {
                parserState = ParserState.INITIALIZE;
                continue;
            }
            if (newLine.matches("(?i)iterate:\\s*\\{")) {
                parserState = ParserState.ITERATE;
                continue;
            }
            if (newLine.matches("(?i)terminate:\\s*\\{")) {
                parserState = ParserState.TERMINATE;
                continue;
            }
            if (!newLine.equals("") && !newLine.matches("\\}\\s*") && parserState != ParserState.STATE_DECLARATION) {
                incompleteQuery += (" " + newLine);
                if (newLine.endsWith(";")) {
                    UdaStatement udaStatement;
                    if (incompleteQuery.trim().matches("(?i)insert into return .*")) {
                        String selectClause = incompleteQuery.trim().substring("insert into return ".length());
                        udaStatement = new UdaStatement(StatementType.RETURN_VALUES, selectClause);
                    } else {
                        udaStatement = new UdaStatement(StatementType.STANDARD, incompleteQuery.trim());
                    }
                    switch (parserState) {
                        case INITIALIZE:
                            initializeStatements.add(udaStatement);
                            break;
                        case ITERATE:
                            iterateStatements.add(udaStatement);
                            break;
                        case TERMINATE:
                            terminateStatements.add(udaStatement);
                            break;
                        default:
                            continue;
                    }
                    incompleteQuery = "";
                }
            }
        }

        try {
            Statement statement = stateStorageConnection.createStatement();
            for (UdaStatement udaStatement : stateDeclarationStatements) {
                String query = udaStatement.query;
                String tableName = query.substring("CREATE TABLE ".length(), query.indexOf("(")).trim();
                String dropQuery = "DROP TABLE IF EXISTS " + tableName;
                statement.executeUpdate(dropQuery);
                statement.executeUpdate(udaStatement.query);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void printState() {
        System.out.println("\nSTATE DECLARATION");
        for (UdaStatement udaStatement : stateDeclarationStatements) {
            System.out.println(udaStatement);
        }
        System.out.println("\nINITIALIZE SECTION");
        for (UdaStatement udaStatement : initializeStatements) {
            System.out.println(udaStatement);
        }
        System.out.println("\nITERATE SECTION");
        for (UdaStatement udaStatement : iterateStatements) {
            System.out.println(udaStatement);
        }
        System.out.println("\nTERMINATE SECTION");
        for (UdaStatement udaStatement : terminateStatements) {
            System.out.println(udaStatement);
        }
    }

    public <T> ArrayList<T> processEvent(Object newEvent, T[] outputType) {
        if (!inflated) {
            throw new IllegalStateException("UdaObject not inflated");
        }

        if (newEvent == null) {
            return null;
        }

        ArrayList<UdaStatement> statementsToExecute = initializeDone ? iterateStatements : initializeStatements;
        Statement statement;
        try {
            statement = stateStorageConnection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        int lastQueryNumRows = Integer.MIN_VALUE;
        ArrayList<T> returnValues = new ArrayList<>();
        for (UdaStatement udaStatement : statementsToExecute) {
            String valReplacedQuery = replaceQueryParamsWithVals(udaStatement.query, newEvent);

            // Replace "ROWSMODIFIED" with number of rows modified in previous statement
            valReplacedQuery = valReplacedQuery.replaceAll("ROWSMODIFIED", "" + lastQueryNumRows);

            switch (udaStatement.statementType) {
                case STANDARD:
                    try {
                        lastQueryNumRows = statement.executeUpdate(valReplacedQuery);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        return null;
                    }
                    break;
                case RETURN_VALUES:
                    try {
                        ResultSet resultSet = statement.executeQuery(valReplacedQuery);
                        returnValues.addAll(getObjectsFromResultSet(resultSet, outputType));
                    } catch (SQLException e) {
                        e.printStackTrace();
                        return null;
                    }
                    break;
            }
        }

        if (!initializeDone)
            initializeDone = true;

        return returnValues;
    }

    public Map<String, Field> getInputFieldMap() {
        return Collections.unmodifiableMap(inputFieldMap);
    }

    public Map<String, Field> getOutputFieldMap() {
        return Collections.unmodifiableMap(outputFieldMap);
    }

    public Class getInputType() {
        return spec.inputType;
    }

    public Class getOutputType() {
        return spec.outputType;
    }

    private String replaceQueryParamsWithVals(String query, Object input) {
        // Ignore column aliases "AS"
        try {
            for (Field field : inputFields) {
                String fieldName = field.getName();
                if (field.getType() == int.class) {
                    int val = field.getInt(input);
                    query = query.replaceAll(fieldName, "" + val);
                } else if (field.getType() == double.class) {
                    double val = field.getDouble(input);
                    query = query.replaceAll(fieldName, "" + val);
                } else if (field.getType() == String.class) {
                    String val = field.get(input).toString();
                    query = query.replaceAll(fieldName, "'" + val + "'");
                }
            }
            return query;
        } catch (IllegalAccessException iae) {
            iae.printStackTrace();
        }
        return null;
    }

    private <T> ArrayList<T> getObjectsFromResultSet(ResultSet resultSet, T[] outputType) {
        try {
            ArrayList<T> output = new ArrayList<T>();
            while (resultSet.next()) {
                Object outputOb = spec.outputType.newInstance();
                for (Field field : outputFields) {
                    String fieldName = field.getName();
                    if (field.getType() == int.class) {
                        int val = resultSet.getInt(fieldName);
                        field.setInt(outputOb, val);
                    } else if (field.getType() == double.class) {
                        double val = resultSet.getDouble(fieldName);
                        field.setDouble(outputOb, val);
                    } else if (field.getType() == String.class) {
                        String val = resultSet.getString(fieldName);
                        field.set(outputOb, val);
                    }
                }
                output.add((T) outputOb);
            }
            return output;
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IllegalAccessException iae) {
            iae.printStackTrace();
        } catch (InstantiationException ie) {
            ie.printStackTrace();
        }
        return null;
    }

    private class UdaStatement {
        public StatementType statementType;
        public String query;

        public UdaStatement(StatementType statementType, String query) {
            this.statementType = statementType;
            this.query = query;
        }

        public String toString() {
            return String.format("[%s] %s", statementType, query);
        }
    }

    private enum StatementType {
        UNKNOWN,
        STANDARD,
        STATE_DECLARATION,
        RETURN_VALUES;

        public String toString() {
            switch (this) {
                case UNKNOWN:
                    return "StatementType: Unknown";
                case STANDARD:
                    return "StatementType: Standard";
                case STATE_DECLARATION:
                    return "StatementType: State Declaration";
                case RETURN_VALUES:
                    return "StatementType: Return values";
                default:
                    return null;
            }
        }
    }

    private enum ParserState {
        UNKNOWN,
        STATE_DECLARATION,
        INITIALIZE,
        ITERATE,
        TERMINATE
    }
}
