package com.ucla.streams_uda.state_storage;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Interface needed by UdaManager to determine what Connection to use to store state information.
 */
public abstract class IStateStorageConnectionProvider {

    private String queryCreateTable = "CREATE TABLE TestTable (num INT)";
    private String queryInsertRows = "INSERT INTO TestTable VALUES (1), (2), (3)";
    private String queryDeleteTable = "DROP TABLE IF EXISTS TestTable";

    /**
     * Return a valid Sql Connection object
     *
     * @return a valid Sql Connection object
     */
    public abstract Connection getConnection();

    /**
     * Test if the connection works
     *
     * @return true if connection works
     */
    public boolean test() {
        Connection connection = getConnection();
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(queryCreateTable);
            statement.executeUpdate(queryInsertRows);
            statement.executeUpdate(queryDeleteTable);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
}
