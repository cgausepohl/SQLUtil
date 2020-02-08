package com.binrock.sqlutil;

import java.sql.SQLException;
import java.util.Hashtable;

public class Query {
	private SQLUtilInterface sql;
	private String selectStmt;
	private Row[] rows;

	public Query(SQLUtilInterface sql, String selectStmt) throws SQLException {
		this.sql = sql;
		this.selectStmt = selectStmt;
	}

	public Query execute() throws SQLException {
		return execute(null, null);
	}

	public Query execute(Object[] bindVariables) throws SQLException {
		return execute(bindVariables, null);
	}

	public Query executeVarArgs(Object... bindVariables) throws SQLException {
		return execute(bindVariables, null);
	}

	public Query execute(Object[] bindVariables,  int[] bindTypes) throws SQLException {
	    Hashtable<String, Integer> colName2Idx = new Hashtable<>();
		rows = sql.getRows(colName2Idx, selectStmt, bindVariables, bindTypes);
		return this;
	}
	
	public Row[] getRows() {
		return rows;
	}

}