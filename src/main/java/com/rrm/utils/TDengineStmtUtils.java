package com.rrm.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.rrm.Sink2TDengine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TDengineStmtUtils {

    Statement statement;
    private Connection connection;
    private static volatile DataSource dataSource;

    static {
//        Properties properties = new Properties();
//        connection = DriverManager.getConnection(PropertiesUtils.readProperty("tdengine.url"), properties);
//        statement = connection.createStatement();
        if (dataSource == null) {
            synchronized (Sink2TDengine.class) {
                if (dataSource == null) {
                    DruidDataSource druidDataSource = new DruidDataSource();
                    druidDataSource.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
                    druidDataSource.setUrl(PropertiesUtils.readProperty("tdengine.url"));
                    druidDataSource.setUsername(PropertiesUtils.readProperty("tdengine.username"));
                    druidDataSource.setPassword(PropertiesUtils.readProperty("tdengine.password"));
                    druidDataSource.setInitialSize(Integer.valueOf(PropertiesUtils.readProperty("tdengine.initialSize")));
                    druidDataSource.setMinIdle(Integer.valueOf(PropertiesUtils.readProperty("tdengine.minIdle")));
                    druidDataSource.setMaxActive(Integer.valueOf(PropertiesUtils.readProperty("tdengine.maxActive")));
                    druidDataSource.setMaxWait(Integer.valueOf(PropertiesUtils.readProperty("tdengine.maxWait")));
                    druidDataSource.setValidationQueryTimeout(Integer.valueOf(PropertiesUtils.readProperty("tdengine.validationQueryTimeout")));
                    druidDataSource.setTimeBetweenEvictionRunsMillis(Integer.valueOf(PropertiesUtils.readProperty("tdengine.timeBetweenEvictionRunsMillis")));
                    druidDataSource.setValidationQuery("select server_status()");
                    dataSource = druidDataSource;
                }
            }
        }
    }

    public static Statement getStmt() throws SQLException{
        return dataSource.getConnection().createStatement();
    }

    public static Connection getConnection() throws SQLException{
        return dataSource.getConnection();
    }
}
