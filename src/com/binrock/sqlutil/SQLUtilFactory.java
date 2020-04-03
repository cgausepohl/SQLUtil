package com.binrock.sqlutil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.binrock.sqlutil.impl.SQLUtil;

public class SQLUtilFactory {

	// todo: create enum for that
	public static final boolean AUTOCOMMIT_ON = true;
	public static final boolean AUTOCOMMIT_OFF = false;

	// todo: create enum for that
	public static final boolean RWMODE_READONLY = true;
	public static final boolean RWMODE_READWRITE = false;

	// create with a Connection
	public static SQLUtilInterface createSQLUtil(Connection con) throws SQLException {
		return createSQLUtil(con, AUTOCOMMIT_ON, Connection.TRANSACTION_READ_COMMITTED);
	}

	public static SQLUtilInterface createSQLUtil(
				Connection con, boolean doAutoCommit, int txIsolationLevel) throws SQLException {
		SQLUtilInterface sql = new SQLUtil(con);  //FIXME: generic classes shouldnt access impl classes
		sql.getConnection().setAutoCommit(doAutoCommit);
		sql.getConnection().setTransactionIsolation(txIsolationLevel);
		return sql;
	}

	// simple: connectstring, user, password. Settings like autoCommit, readonly
	// will be the defaultvalues of the driver
	public static SQLUtilInterface createSQLUtil(String jdbc, String user, String password) throws SQLException {
		Connection con = DriverManager.getConnection(jdbc, user, password);
		return createSQLUtil(con);
	}

	// full control over the created connection
	public static SQLUtilInterface createSQLUtil(String jdbc, String user, String password, boolean readOnly,
			boolean doAutoCommit, int txIsolationLevel) throws SQLException {
		SQLUtilInterface sql=null;
		sql = createSQLUtil(jdbc, user, password);
		sql.getConnection().setAutoCommit(doAutoCommit);
		sql.getConnection().setTransactionIsolation(txIsolationLevel);
	    return sql;
	}

}
