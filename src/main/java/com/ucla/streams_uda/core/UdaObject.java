package com.ucla.streams_uda.core;

import com.ucla.streams_uda.state_storage.IStateStorageConnectionProvider;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Implementation object for a specific UDA
 * <p>
 * NOTE: This class needs to be serializable in order to be potentially wrapped in a Storm bolt.
 * This necessitates the need for the inflate method which initializes the members that cannot be serialized.
 */
public class UdaObject implements Serializable {
    // The spec for the UDA
    private UdaSpec spec;
    // Connection object to use for storing the state table
    private Connection stateStorageConnection;
    // Provides the connection object to be used
    private IStateStorageConnectionProvider stateStorageConnectionProvider;
    // Whether the object has yet been initialized
    private boolean inflated = false;

    // Statements in different sections of the UDA
    private ArrayList<UdaStatement> stateDeclarationStatements = new ArrayList<>();
    private ArrayList<UdaStatement> initializeStatements = new ArrayList<>();
    private ArrayList<UdaStatement> iterateStatements = new ArrayList<>();
    private ArrayList<UdaStatement> terminateStatements = new ArrayList<>(); // * Not used/implemented at the moment

    // Whether initialize section of the UDA has been implemented
    private boolean initializeDone = false;

    // Fields in the input and output objects
    private Field[] inputFields;
    private Field[] outputFields;

    // Method in the input object that do not take any arguments but return a value (i.e. they act as properties)
    private LinkedList<Method> inputProperties;

    // Map from name to input/output fields
    private Map<String, Field> inputFieldMap;
    private Map<String, Field> outputFieldMap;

    /**
     * CTor
     *
     * @param udaSpec                        spec object for the UDA
     * @param stateStorageConnectionProvider Provider for the state SQL connection
     */
    public UdaObject(UdaSpec udaSpec, IStateStorageConnectionProvider stateStorageConnectionProvider) {
        this.spec = udaSpec;
        this.stateStorageConnectionProvider = stateStorageConnectionProvider;
    }

    /**
     * Inflate the UDA object.
     * <p>
     * This method initilizes parts of the UdaObject that cannot be serialized. Serializing the UdaObject is
     * necessary when it is wrapped in a Storm Bolt.
     */
    public void inflate() {
        // Get the SQL connection object for the state table
        stateStorageConnection = stateStorageConnectionProvider.getConnection();

        // Set of input/output object fields
        inputFields = spec.inputType.getFields();
        outputFields = spec.outputType.getFields();

        // Method in the input object that do not take any arguments but returns a value (i.e. they act as properties)
        inputProperties = new LinkedList<>();
        for (Method method : spec.inputType.getMethods()) {
            if (method.getParameterCount() == 0 /* No parameters */
                    && method.getReturnType() != void.class /* Returns a value */) {
                inputProperties.add(method);
            }
        }

        // Create a map from field name to Field for quick access
        inputFieldMap = new HashMap<>();
        outputFieldMap = new HashMap<>();
        for (Field inputField : inputFields) {
            inputFieldMap.put(inputField.getName(), inputField);
        }
        for (Field outputField : outputFields) {
            outputFieldMap.put(outputField.getName(), outputField);
        }

        // Read the UDA definition file
        Scanner sc = null;
        try {
            sc = new Scanner(new FileReader(spec.fileName));
        } catch (FileNotFoundException e) {
            System.err.printf("Could not find file %s for aggregate %s%n", spec.fileName, spec.udaName);
            e.printStackTrace();
            return;
        }

        // The definition starts with state table definitions
        ParserState parserState = ParserState.STATE_DECLARATION;

        // Used to append query lines till a ";" is found. Everything between semi-colons is considered a single query.
        String incompleteQuery = "";

        while (sc.hasNextLine()) {
            String newLine = sc.nextLine().trim();

            // Start of UDA (ignore line)
            if (newLine.matches("(?i)aggregate .*") && parserState == ParserState.STATE_DECLARATION) {
                continue;
            }

            // Start of initialize block
            if (newLine.matches("(?i)initialize:\\s*\\{")) {
                parserState = ParserState.INITIALIZE;
                continue;
            }

            // Start of iterate block
            if (newLine.matches("(?i)iterate:\\s*\\{")) {
                parserState = ParserState.ITERATE;
                continue;
            }

            // Start of terminate block
            if (newLine.matches("(?i)terminate:\\s*\\{")) {
                parserState = ParserState.TERMINATE;
                continue;
            }

            // Append line to current query. If semicolon, query is considered complete.
            if (!newLine.equals("") && !newLine.matches("\\}\\s*")) {
                incompleteQuery += (" " + newLine);
                if (newLine.endsWith(";")) {
                    incompleteQuery = incompleteQuery.trim();
                    UdaStatement udaStatement;

                    if (incompleteQuery.matches("(?i)table .*") && parserState == parserState.STATE_DECLARATION) {
                        udaStatement = new UdaStatement(
                                StatementType.STATE_DECLARATION,
                                "CREATE " + incompleteQuery);
                    } else if (incompleteQuery.matches("(?i)insert into return .*")) {
                        String selectClause = incompleteQuery.trim().substring("insert into return ".length());
                        udaStatement = new UdaStatement(StatementType.RETURN_VALUES, selectClause);
                    } else {
                        udaStatement = new UdaStatement(StatementType.STANDARD, incompleteQuery.trim());
                    }

                    switch (parserState) {
                        case STATE_DECLARATION:
                            stateDeclarationStatements.add(udaStatement);
                            break;
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
            return;
        }

        inflated = true;
    }

    /**
     * Process a new incoming event
     *
     * @param newEvent   new event object
     * @param outputType a possibly empty array of type matching the output object. Used to avoid putting the burden
     *                   of typecasting on the client.
     * @param <T>        Output object type
     * @return a ArrayList of the output objects generated after this event
     */
    public <T> ArrayList<T> processEvent(Object newEvent, T[] outputType) {
        // Inflate if not already inflated
        if (!inflated) {
            System.out.println("UdaObject not inflated! Inflating...");
            inflate();
        }

        if (newEvent == null) {
            return null;
        }

        // Decide whether to execute initialize or iterate section
        ArrayList<UdaStatement> statementsToExecute = initializeDone ? iterateStatements : initializeStatements;

        // Initialize a statement object
        Statement statement;
        try {
            statement = stateStorageConnection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        // Store number of rows affected by previous query. Can be accessed using the "ROWSMODIFIED" variable in the
        // UDA Definition.
        int lastQueryNumRows = Integer.MIN_VALUE;

        // Objects to be returned
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

    /**
     * Return map from Field name to field in input object
     *
     * @return HashMap from Field name to input Field
     */
    public Map<String, Field> getInputFieldMap() {
        return Collections.unmodifiableMap(inputFieldMap);
    }

    /**
     * Return map from Field name to Field in output object
     *
     * @return HashMap from Field name to output Field
     */
    public Map<String, Field> getOutputFieldMap() {
        return Collections.unmodifiableMap(outputFieldMap);
    }

    /**
     * Return Class object of the input type
     *
     * @return type of input object
     */
    public Class getInputType() {
        return spec.inputType;
    }

    /**
     * Return Class object of the output type
     *
     * @return type of the output object
     */
    public Class getOutputType() {
        return spec.outputType;
    }

    /**
     * Replace terms in the query that match any of the input object field names and replaces them with the value in
     * the input object
     *
     * @param query the query to replace parameters
     * @param input the input object
     * @return the query with parameter names replaced with values
     */
    private String replaceQueryParamsWithVals(String query, Object input) {
        try {
            for (Field field : inputFields) {
                String fieldName = "@" + field.getName();
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
            for (Method method : inputProperties) {
                String propertyName = "@" + method.getName();
                if (method.getReturnType() == int.class) {
                    int val = Integer.parseInt(method.invoke(input).toString());
                    query = query.replaceAll(propertyName, "" + val);
                } else if (method.getReturnType() == double.class) {
                    double val = Double.parseDouble(method.invoke(input).toString());
                    query = query.replaceAll(propertyName, "" + val);
                } else if (method.getReturnType() == String.class) {
                    String val = method.invoke(input).toString();
                    query = query.replaceAll(propertyName, "'" + val + "'");
                }
            }
            return query;
        } catch (IllegalAccessException iae) {
            iae.printStackTrace();
        } catch (InvocationTargetException ite) {
            ite.printStackTrace();
        }
        return null;
    }

    /**
     * Take a result set and convert into list of output objects.
     * <p>
     * The names of the columns are matched to variables in the output object with the same name. This makes it
     * necessary to name output columns in any of the return statements using the "AS" keyword.
     *
     * @param resultSet  the result set to convert
     * @param outputType a possibly empty array of type matching the output object. Used to avoid putting the burden
     *                   of typecasting on the client.
     * @param <T>        The type of the output object
     * @return a ArrayList of output objects
     */
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

    // Wrapper class to store a UdaStaement along with its type
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

    // Type of UdaStatement
    private enum StatementType {
        UNKNOWN,
        STANDARD,          /* Standard SQL Query */
        STATE_DECLARATION, /* State table declaration. Starts with "TABLE ..." before the INITIALIZE section */
        RETURN_VALUES;     /* Return statements that start with "INSERT INTO RETURN ..." */

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

    // State of the parser
    private enum ParserState {
        UNKNOWN,
        STATE_DECLARATION,
        INITIALIZE,
        ITERATE,
        TERMINATE
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
}
