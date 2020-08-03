package com.binrock.sqlutil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.binrock.sqlutil.impl.SQLUtil;

public class SQLUtilFactory {

    public enum AutoCommit {ON, OFF}

    // todo: create enum for that
    public enum RWMode {READONLY, READWRITE}

    // create with a Connection
    public static SQLUtilInterface createSQLUtil(Connection con) throws SQLException {
        return createSQLUtil(con, AutoCommit.ON, Connection.TRANSACTION_READ_COMMITTED);
    }

    // txIsolationLevel: see Connection.TRANSACTION_*
    public static SQLUtilInterface createSQLUtil(Connection con, AutoCommit autoCommit,
            int txIsolationLevel) throws SQLException {
        SQLUtilInterface sql = new SQLUtil(con); //FIXME: generic classes shouldnt access impl classes
        if (autoCommit==AutoCommit.OFF)
            sql.getConnection().setAutoCommit(false);
        else
            sql.getConnection().setAutoCommit(true);
        sql.getConnection().setTransactionIsolation(txIsolationLevel);
        return sql;
    }

    // simple: connectstring, user, password. Settings like autoCommit, readonly
    // will be the defaultvalues of the driver
    public static SQLUtilInterface createSQLUtil(String jdbc, String user, String password)
            throws SQLException {
        Connection con = DriverManager.getConnection(jdbc, user, password);
        return createSQLUtil(con);
    }

    // full control over the created connection
    // txIsolationLevel: see Connection.TRANSACTION_*
    public static SQLUtilInterface createSQLUtil(String jdbc, String user, String password,
            RWMode rwmode, AutoCommit autoCommit, int txIsolationLevel) throws SQLException {
        SQLUtilInterface sql = null;
        sql = createSQLUtil(jdbc, user, password);
        if (rwmode==RWMode.READWRITE)
            sql.getConnection().setReadOnly(false);
        else
            sql.getConnection().setReadOnly(true);
        if (autoCommit==AutoCommit.OFF)
            sql.getConnection().setAutoCommit(false);
        else
            sql.getConnection().setAutoCommit(true);
        sql.getConnection().setTransactionIsolation(txIsolationLevel);
        return sql;
    }

}
