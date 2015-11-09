package com.ucla.streams_uda.state_storage;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A implementation of the IStateStorageConnectionProvider for MySQL based state tables.
 */
public class MysqlStorageConnectionProvider extends IStateStorageConnectionProvider implements Serializable {
    // MySQL driver
    private final String driver = "com.mysql.jdbc.Driver";
    // Database URL
    private final String dbUrl;
    // Database User
    private final String user;
    // Database password
    private final String pass;

    /**
     * Ctor
     *
     * @param url      url to the database
     * @param dbName   name of the database
     * @param username username
     * @param host     host name
     * @param password password for the database
     */
    public MysqlStorageConnectionProvider(String url, String dbName, String username, String host, String password) {
        // DB URL is URL to database + Database name
        dbUrl = url.trim() + dbName.trim();

        if (host.trim().isEmpty())
            user = String.format("%s@%s", username.trim(), host.trim());
        else
            user = username.trim();

        pass = password;
    }

    /**
     * Get a connection object
     *
     * @return a SQL connection object
     */
    @Override
    public Connection getConnection() {

        try {
            Class.forName(driver);
            Connection connection = DriverManager.getConnection(dbUrl, user, pass);
            return connection;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }
}
