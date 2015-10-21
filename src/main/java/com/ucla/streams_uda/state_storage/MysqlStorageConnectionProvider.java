package com.ucla.streams_uda.state_storage;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by sumit on 10/8/15.
 */
public class MysqlStorageConnectionProvider extends IStateStorageConnectionProvider implements Serializable {

    private final String driver = "com.mysql.jdbc.Driver";
    private final String dbUrl;
    private final String user;
    private final String pass;

    public MysqlStorageConnectionProvider(String url, String dbName, String username, String host, String password) {
        dbUrl = url.trim() + dbName.trim();

        if (host.trim().isEmpty())
            user = String.format("%s@%s", username.trim(), host.trim());
        else
            user = username.trim();

        pass = password;
    }

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
