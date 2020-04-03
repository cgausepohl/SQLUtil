package com.binrock.sqlutil;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.List;

/**
 * Purpose of SQLUtil is to create short, readable and easy to write code that uses JDBC.
 * Normally JDBC, if used correctly, produces a lot of Lines Of Code (LOC), just to
 * control resources, check errorcodes, exceptions and nulls.
 *
 * about type mapping:
 * there are several methods with parameters like "String stmt, Object...bindVariables"
 * or "String stmt, Object[] bindVariables". All values in bindVariables must be not null.
 * Allowed and mapped Types are:
 * Byte, Short, Integer, Long         => Statement.setLong()
 * BigDecimal                         => Statement.setBigDecimal()
 * float, double                      => Statement.setDouble()
 * Character, String                  => Statement.setString()
 * Boolean                            => Statement.setBoolean()
 * byte[]                             => Statement.setBytes()
 * java.util.Date, java.sql.Timestamp => Statement.setTimestamp()
 * java.sql.Time                      => Statement.setTime()
 *
 * Not listed types will not be mapped automatically. If you need another mapping you can try to convert it from another value:
 * Example: "update x set col=to_date(?,?)", new String("2018-12-24 18:00:00"), new String("YYYY-MM-DD HH24:MI:SS")
 */

public interface SQLUtilInterface {

	// Connection Management
	//
	// must be called before your program ends, or no further SQLs will be executed
	public void closeConnection();
	// Connection gives you full access
	public Connection getConnection();
	// confirm changes
	public void commit() throws SQLException;
	public void commitSilent();
	// undo all changes since last commit/rollback or connection-creation
	public void rollback() throws SQLException;
	public void rollbackSilent();
	// call prepare, returns false if error occurs
	public boolean isPreparable(String sqlStmt);
	// some JDBC-methods are using a Calendar object.
	public void setCalendar(Calendar c);
	// helpers
	public void closeSilent(ResultSet rs);
	public void closeSilent(Statement stmt);
	public ResultSetMetaData getPreviousMetaData();
	public int[] getPreviousRowSQLTypes();
	public void setFetchSize(int fetchSize);
	public int getFetchSize();
    public void enablePreparedStatementCache(boolean enable) throws SQLException;
    public void setPreparedStatementCacheSize(int cacheSize) throws SQLException;

	// log management. Use null to suppress output.
    public void setStdout(PrintStream stdout);
    public void setStderr(PrintStream stderr);

    // exec Data Manipulation Language (DML) / Data Definition Language (DDL) directly
	//
	// execute ddl (things like "create table(id numeric)" etc.)
	public void executeDDL(String ddlStmt) throws SQLException;
	// execute dml (update, insert, delete, merge)
	public Integer executeDML(String dmlStmt) throws SQLException;
	// exec once, ? will be replaced with varargs
	public Integer executeDMLVarArgs(String dmlStmt, Object... bindVariables) throws SQLException;
	// exec once, ? will be replaced with array
	public Integer executeDML(String dmlStmt, final Object[] bindVariables) throws SQLException;
	//
	public Integer executeDML(String dmlStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
    // executes dmlStmt n times, where n=batchValues.length
	public int[] executeDMLBatch(String dmlStmt, final List<Object[]> batchValues, final int[] bindTypes) throws SQLException;
    // executes dmlStmt n times, where n=batchValues.length
	public int[] executeDMLBatch(String dmlStmt, final Row[] rows, final int[] bindTypes) throws SQLException;

	// exec Stored Procedure (SP)
	//
	// exec sp
	public void executeSP(String call) throws SQLException;
	// exec sp, ? will be replaced with varargs
	public void executeSPVarArgs(String call, final Object... bindVariables) throws SQLException;
	// exec sp, ? will be replaced with array
	public void executeSP(String call, final Object[] bindVariables) throws SQLException;
	//
	public void executeSP(String call, final Object[] bindVariables, final int[] bindTypes) throws SQLException;


	// Complete ResultSets
	// Returns an array of Row
	//
	public Row[] getRowsVarArgs(String selectStmt,  Object...bindVariables) throws SQLException;
	public Row[] getRows(String selectStmt) throws SQLException;
	public Row[] getRows(String selectStmt, Object[] bindVariables) throws SQLException;
	public Row[] getRows(Hashtable<String, Integer> columnMap, String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;

	// Fetch ResultSet in multiple Chunks
    // Returns an array of Row. Order of call: batchPrepare, while ((rows=batchGetRows())!=null), batchClose
    //
	public void getChunksPrepare(String selectStmt, int batchSize) throws SQLException;
	public Row[] getChunksGetNextRows() throws SQLException;
	public void getChunksClose();

	// Timestamp: Use this for all Date-alike subtypes
	//
	// execute select, first column must be convertible to Timestamp, only one resultrow allowed
	public Timestamp getTimestamp(String selectStmt) throws SQLException;
	public Timestamp getTimestampVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public Timestamp getTimestamp(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public Timestamp getTimestamp(String selectStmt, final Object[] bindVariables) throws SQLException;
	public Timestamp[] getTimestamps(String selectStmt) throws SQLException;
	public Timestamp[] getTimestampsVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public Timestamp[] getTimestamps(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public Timestamp[] getTimestamps(String selectStmt, final Object[] bindVariables) throws SQLException;

	// BigDecimal. Use this for all big/unknown numbers
	//
	public BigDecimal getBigDecimal(String selectStmt) throws SQLException;
	public BigDecimal getBigDecimalVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public BigDecimal getBigDecimal(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public BigDecimal getBigDecimal(String selectStmt, final Object[] bindVariables) throws SQLException;
	public BigDecimal[] getBigDecimals(String selectStmt) throws SQLException;
	public BigDecimal[] getBigDecimalsVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public BigDecimal[] getBigDecimals(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public BigDecimal[] getBigDecimals(String selectStmt, final Object[] bindVariables) throws SQLException;

	// Long(s). Use this for all Integer-alike subtypes
	//
	public Long getLong(String selectStmt) throws SQLException;
	public Long getLongVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public Long getLong(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public Long getLong(String selectStmt, final Object[] bindVariables) throws SQLException;
	public Long[] getLongs(String selectStmt) throws SQLException;
	public Long[] getLongsVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public Long[] getLongs(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public Long[] getLongs(String selectStmt, final Object[] bindVariables) throws SQLException;

	// Double. Use this for all Floatingpoint-alike subtypes
	//
	public Double getDouble(String selectStmt) throws SQLException;
	public Double getDoubleVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public Double getDouble(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public Double getDouble(String selectStmt, final Object[] bindVariables) throws SQLException;
	public Double[] getDoubles(String selectStmt) throws SQLException;
	public Double[] getDoublesVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public Double[] getDoubles(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public Double[] getDoubles(String selectStmt, final Object[] bindVariables) throws SQLException;

	// String. Use this for all Strings or formatted values
	//
	public String getString(String selectStmt) throws SQLException;
	public String getStringVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public String getString(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public String getString(String selectStmt, final Object[] bindVariables) throws SQLException;
	public String[] getStrings(String selectStmt) throws SQLException;
	public String[] getStringsVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public String[] getStrings(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public String[] getStrings(String selectStmt, final Object[] bindVariables) throws SQLException;

	// Raw, bytearray. Use this for binary content
	//
	public byte[] getRaw(String selectStmt) throws SQLException;
	public byte[] getRawVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public byte[] getRaw(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public byte[] getRaw(String selectStmt, final Object[] bindVariables) throws SQLException;
	public byte[][] getRaws(String selectStmt) throws SQLException;
	public byte[][] getRawsVarArgs(String selectStmt, final Object...bindVariables) throws SQLException;
	public byte[][] getRaws(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	public byte[][] getRaws(String selectStmt, final Object[] bindVariables) throws SQLException;

	// Statistics and Timings
	//
	public AuditInterface getAudit();
/*	public void enableTimings(boolean enable);
    public Hashtable<String, List<AuditRecord>> getExecutionStatistcs();
    public void flushExecutionStatistcs();
    public void flushExecutionStatistcs(long olderThanSec);
    // level 0: all executions, 1: grouped by sql+bindvars, 2: grouped by sql, 3: top10-all-time, 4:top10-last24h, 5: top10-last1h
    // sqlContains: if !=null only summarize where sql contains sqlContains (case insensitive)
	public void printExecutionStatistcsSummary(PrintStream out, int level, String sqlContains);
	*/
}
