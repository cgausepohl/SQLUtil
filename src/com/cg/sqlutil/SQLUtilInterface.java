/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/SQLUtil
 */
package com.cg.sqlutil;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
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

    public enum DBProduct {
        POSTGRESQL, MYSQL, ORACLE, MSSQLSERVER, GENERIC
    }

    // Connection Management
    //
    // must be called before your program ends, or no further SQLs will be executed
    void closeConnection();

    // Connection gives you full access
    Connection getConnection();

    // confirm changes
    void commit() throws SQLException;

    void commitSilent();

    // undo all changes since last commit/rollback or connection-creation
    void rollback() throws SQLException;

    void rollbackSilent();

    // call prepare, returns false if error occurs
    boolean isPreparable(String sqlStmt);

    // some JDBC-methods are using a Calendar object.
    void setCalendar(Calendar c);

    // helpers
    void closeSilent(ResultSet rs);

    void closeSilent(Statement stmt);

    ResultSetMetaData getPreviousMetaData();

    int[] getPreviousRowSQLTypes();

    void setFetchSize(int fetchSize);

    int getFetchSize();

    void enablePreparedStatementCache(boolean enable) throws SQLException;

    void setPreparedStatementCacheSize(int cacheSize) throws SQLException;

    DBProduct getDBProduct();

    // log management. Use null to suppress output.
    void setStdout(PrintStream stdout);

    void setStderr(PrintStream stderr);

    // exec Data Manipulation Language (DML) / Data Definition Language (DDL) directly
    //
    // execute ddl (things like "create table(id numeric)" etc.)
    void executeDDL(String ddlStmt) throws SQLException;

    // like executeDDL, but no exception will be thrown.
    void executeDDLSilent(String ddlStmt);

    // execute dml (update, insert, delete, merge)
    Integer executeDML(String dmlStmt) throws SQLException;

    // exec once, ? will be replaced with varargs
    Integer executeDMLVarArgs(String dmlStmt, Object... bindVariables) throws SQLException;

    // exec once, ? will be replaced with array
    Integer executeDML(String dmlStmt, final Object[] bindVariables) throws SQLException;

    //
    Integer executeDML(String dmlStmt, final Object[] bindVariables, final int[] bindTypes)
            throws SQLException;

    // executes dmlStmt n times, where n=batchValues.length
    int[] executeDMLBatch(String dmlStmt, final List<Object[]> batchValues,
            final int[] bindTypes) throws SQLException;

    // executes dmlStmt n times, where n=batchValues.length
    int[] executeDMLBatch(String dmlStmt, final Row[] rows, final int[] bindTypes)
            throws SQLException;

    // exec Stored Procedure (SP)
    //
    // exec sp
    void executeSP(String call) throws SQLException;

    // exec sp, ? will be replaced with varargs
    void executeSPVarArgs(String call, final Object... bindVariables) throws SQLException;

    // exec sp, ? will be replaced with array
    void executeSP(String call, final Object[] bindVariables) throws SQLException;

    //
    void executeSP(String call, final Object[] bindVariables, final int[] bindTypes)
            throws SQLException;

    // Retrieves one row
    //
    Row getRowVarArgs(String selectStmt, Object... bindVariables) throws SQLException;

    Row getRow(String selectStmt) throws SQLException;

    Row getRow(String selectStmt, Object[] bindVariables) throws SQLException;

    Row getRow(/* why columnmap here?Hashtable<String, Integer> columnMap, */String selectStmt,
            final Object[] bindVariables, final int[] bindTypes) throws SQLException;

    // Complete ResultSets
    // Returns an array of Row
    //
    Row[] getRowsVarArgs(String selectStmt, Object... bindVariables) throws SQLException;

    Row[] getRows(String selectStmt) throws SQLException;

    Row[] getRows(String selectStmt, Object[] bindVariables) throws SQLException;

    Row[] getRows(String selectStmt, final Object[] bindVariables, final int[] bindTypes) throws SQLException;
    /* why columnMap here?
    Row[] getRows(Hashtable<String, Integer> columnMap, String selectStmt,
            final Object[] bindVariables, final int[] bindTypes) throws SQLException;
	*/
    
    // Fetch ResultSet in multiple Chunks
    // Returns an array of Row. Order of call: batchPrepare, while ((rows=batchGetRows())!=null), batchClose
    //
    void getChunksPrepare(String selectStmt, int batchSize) throws SQLException;

    Row[] getChunksGetNextRows() throws SQLException;

    void getChunksClose();

    // Timestamp: Use this for all Date-alike subtypes
    //
    // execute select, first column must be convertible to Timestamp, only one resultrow allowed
    Timestamp getTimestamp(String selectStmt) throws SQLException;

    Timestamp getTimestampVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    Timestamp getTimestamp(String selectStmt, final Object[] bindVariables,
            final int[] bindTypes) throws SQLException;

    Timestamp getTimestamp(String selectStmt, final Object[] bindVariables)
            throws SQLException;

    Timestamp[] getTimestamps(String selectStmt) throws SQLException;

    Timestamp[] getTimestampsVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    Timestamp[] getTimestamps(String selectStmt, final Object[] bindVariables,
            final int[] bindTypes) throws SQLException;

    Timestamp[] getTimestamps(String selectStmt, final Object[] bindVariables)
            throws SQLException;

    // BigDecimal. Use this for all big/unknown numbers
    //
    BigDecimal getBigDecimal(String selectStmt) throws SQLException;

    BigDecimal getBigDecimalVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    BigDecimal getBigDecimal(String selectStmt, final Object[] bindVariables,
            final int[] bindTypes) throws SQLException;

    BigDecimal getBigDecimal(String selectStmt, final Object[] bindVariables)
            throws SQLException;

    BigDecimal[] getBigDecimals(String selectStmt) throws SQLException;

    BigDecimal[] getBigDecimalsVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    BigDecimal[] getBigDecimals(String selectStmt, final Object[] bindVariables,
            final int[] bindTypes) throws SQLException;

    BigDecimal[] getBigDecimals(String selectStmt, final Object[] bindVariables)
            throws SQLException;

    // Long(s). Use this for all Integer-alike subtypes
    //
    Long getLong(String selectStmt) throws SQLException;

    Long getLongVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    Long getLong(String selectStmt, final Object[] bindVariables, final int[] bindTypes)
            throws SQLException;

    Long getLong(String selectStmt, final Object[] bindVariables) throws SQLException;

    Long[] getLongs(String selectStmt) throws SQLException;

    Long[] getLongsVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    Long[] getLongs(String selectStmt, final Object[] bindVariables, final int[] bindTypes)
            throws SQLException;

    Long[] getLongs(String selectStmt, final Object[] bindVariables) throws SQLException;

    // Double. Use this for all Floatingpoint-alike subtypes
    //
    Double getDouble(String selectStmt) throws SQLException;

    Double getDoubleVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    Double getDouble(String selectStmt, final Object[] bindVariables, final int[] bindTypes)
            throws SQLException;

    Double getDouble(String selectStmt, final Object[] bindVariables) throws SQLException;

    Double[] getDoubles(String selectStmt) throws SQLException;

    Double[] getDoublesVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    Double[] getDoubles(String selectStmt, final Object[] bindVariables,
            final int[] bindTypes) throws SQLException;

    Double[] getDoubles(String selectStmt, final Object[] bindVariables) throws SQLException;

    // String. Use this for all Strings or formatted values
    //
    String getString(String selectStmt) throws SQLException;

    String getStringVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    String getString(String selectStmt, final Object[] bindVariables, final int[] bindTypes)
            throws SQLException;

    String getString(String selectStmt, final Object[] bindVariables) throws SQLException;

    String[] getStrings(String selectStmt) throws SQLException;

    String[] getStringsVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    String[] getStrings(String selectStmt, final Object[] bindVariables,
            final int[] bindTypes) throws SQLException;

    String[] getStrings(String selectStmt, final Object[] bindVariables) throws SQLException;

    // Raw, bytearray. Use this for binary content
    //
    byte[] getRaw(String selectStmt) throws SQLException;

    byte[] getRawVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    byte[] getRaw(String selectStmt, final Object[] bindVariables, final int[] bindTypes)
            throws SQLException;

    byte[] getRaw(String selectStmt, final Object[] bindVariables) throws SQLException;

    byte[][] getRaws(String selectStmt) throws SQLException;

    byte[][] getRawsVarArgs(String selectStmt, final Object... bindVariables)
            throws SQLException;

    byte[][] getRaws(String selectStmt, final Object[] bindVariables, final int[] bindTypes)
            throws SQLException;

    byte[][] getRaws(String selectStmt, final Object[] bindVariables) throws SQLException;

    // Statistics and Timings
    //
    AuditInterface getAudit();
    /*	public void enableTimings(boolean enable);
    public Hashtable<String, List<AuditRecord>> getExecutionStatistcs();
    public void flushExecutionStatistcs();
    public void flushExecutionStatistcs(long olderThanSec);
    // level 0: all executions, 1: grouped by sql+bindvars, 2: grouped by sql, 3: top10-all-time, 4:top10-last24h, 5: top10-last1h
    // sqlContains: if !=null only summarize where sql contains sqlContains (case insensitive)
    	public void printExecutionStatistcsSummary(PrintStream out, int level, String sqlContains);
    	*/
    
    /*
     * return the last exec time in ms
     */
    long getLastExecMs();
}
