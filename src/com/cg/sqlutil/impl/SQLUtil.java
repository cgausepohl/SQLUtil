/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/SQLUtil
 */
package com.cg.sqlutil.impl;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.List;

import com.cg.sqlutil.AuditInterface;
import com.cg.sqlutil.Row;
import com.cg.sqlutil.SQLUtilInterface;

public final class SQLUtil implements SQLUtilInterface {

    private Connection con;
    private Calendar calendar;
    private ResultSetMetaData lastMetaData;
    private Integer expectedRows, expectedColumns;
    private int fetchSize = 0;
    private PrintStream stdout = System.out, stderr = System.err;
    private AuditInterface audit = new Audit();
    private long lastExecMs;

    // batch
    private ResultSet batchResultSet;
    private PreparedStatement batchPreparedStatement;
    private int batchColCount, batchBatchSize, batchRowCount;
    private Boolean batchResultSetDone;
    private String batchSelectStmt;
    private int[] batchResultTypes;

    // caches. if hashtable==null, cache is turned off. maxvalues<=0 results in unlimited cache.
    private LRUCache lruStatementCache = null;

    @Override
    public AuditInterface getAudit() {
        return audit;
    }
    
    public long getLastExecMs() {
    	return lastExecMs;
    }
    
    private void calculateExecTimeMs(long t0) {
    	lastExecMs = System.currentTimeMillis()-t0;
    }

    private DBProduct dbProduct = DBProduct.GENERIC;

    public SQLUtil(Connection con) throws SQLException {
        setConnection(con);
    }

    @Override
    public DBProduct getDBProduct() {
        return dbProduct;
    }

    @Override
    public void setStdout(PrintStream stdout) {
        this.stdout = stdout;
    }

    @Override
    public void setStderr(PrintStream stderr) {
        this.stderr = stderr;
    }

    private void error(String msg, Throwable t) {
        log(stderr, msg, t);
    }

    private void info(String msg) {
        log(stdout, msg, null);
    }

    @SuppressWarnings("unused")
    private void info(String msg, Throwable t) {
        log(stdout, msg, t);
    }

    private void log(PrintStream ps, String msg, Throwable t) {
        if (ps == null)
            return;
        ps.println(msg);
        if (t != null)
            t.printStackTrace(ps);
    }

    private PreparedStatement getPreparedStatement(String sql) throws SQLException {
        if (lruStatementCache == null)
            return getConnection().prepareStatement(sql);
        PreparedStatement ps = lruStatementCache.get(sql);
        if (ps != null && ps.isClosed())
            lruStatementCache.remove(sql);
        if (ps == null) {
            ps = getConnection().prepareStatement(sql);
            info("prepare:" + sql);
            lruStatementCache.add(sql, ps);
        }
        return ps;
    }

    @Override
    public void enablePreparedStatementCache(boolean enable) throws SQLException {
        if (enable) {
            if (lruStatementCache == null)
                lruStatementCache = new LRUCache();
        } else {
            if (lruStatementCache != null) {
                lruStatementCache.clear();
                lruStatementCache = null;
            }
        }
    }

    @Override
    // cacheSize<=0 means no limit
    public void setPreparedStatementCacheSize(int cacheSize) throws SQLException {
        if (lruStatementCache == null)
            enablePreparedStatementCache(true);
        lruStatementCache.resize(cacheSize);
    }

    //DEV (gaus20200329: why is this DEV?)
    @Override
    public void getChunksPrepare(String selectStmt, int batchSize) throws SQLException {
        /* prepare PS+RS
         * metaData
         * colcount
         * typeMap?
         */

    	long t0 = System.currentTimeMillis();
        // prepare, bind and execute, no autocommit and readonly are important to stream sql-data with postgres
    	getConnection().setAutoCommit(false);	
    	getConnection().setReadOnly(true);
    	batchPreparedStatement = getPreparedStatement(selectStmt);
        //FIXME?		batchPreparedStatement = BindHelper.bindVariables(batchPreparedStatement, null, null, getCalendar());
    	batchPreparedStatement.setFetchSize(batchSize);	
        batchPreparedStatement.closeOnCompletion();
    	batchResultSet = batchPreparedStatement.executeQuery();
        batchResultSet.setFetchSize(fetchSize);

        lastMetaData = batchResultSet.getMetaData();
        batchColCount = lastMetaData.getColumnCount();
        batchResultTypes = new int[batchColCount];
        for (int i = 1, n = 0; i <= batchColCount; i++, n++) {
            batchResultTypes[n] = lastMetaData.getColumnType(i);
            //System.out.println(n+":"+lastMetaData.getColumnClassName(i)+" "+lastMetaData.getColumnTypeName(i));
            // a little hack. SQL.INTEGER are mapped to java long. Java long must be mapped to SQL.BIGINT
            if (batchResultTypes[n] == Types.INTEGER)
                batchResultTypes[n] = Types.BIGINT;
            // JSON hack postgres	
            if (getDBProduct()==DBProduct.POSTGRESQL && batchResultTypes[n] == 1111) 	
                batchResultTypes[n] = Types.VARCHAR;
        }
        if (expectedColumns != null && expectedColumns != batchColCount) {
            throw new IllegalStateException("Expected columns: " + expectedColumns
                    + ", columns selected: " + batchColCount);
        }

        batchBatchSize = batchSize;
        batchRowCount = 0;
        batchResultSetDone = false;
        batchSelectStmt = selectStmt;
        calculateExecTimeMs(t0);
    }

    @Override
    public Row[] getChunksGetNextRows() throws SQLException {
    	
    	long t0 = System.currentTimeMillis();
        if (batchResultSet == null || batchResultSet.isClosed())
            throw new SQLException(
                    "current iteration not prepared (no call of prepareRowsIterated)");
        if (batchResultSetDone)
            return null;

        int currBatchCounter = 0;
        ArrayList<Row> rowsList = new ArrayList<>(batchBatchSize);
        while (true) {
            if (batchResultSet.next()) {
                Object[] row = convertResultRow2ObjectArray(batchResultSet, batchColCount,
                        batchRowCount, batchResultTypes, batchSelectStmt);
                rowsList.add(new Row(row, /*columnMap*/null));
                batchRowCount++;
                currBatchCounter++;
                if (currBatchCounter >= batchBatchSize)
                    break;
            } else {
                batchResultSetDone = true;
                break;
            }
        }

        if (rowsList.size() == 0)
            return null;

        Row[] rows = new Row[rowsList.size()];
        int i = 0;
        for (Row row : rowsList) {
            rows[i] = row;
            i++;
        }
        calculateExecTimeMs(t0);
        return rows;
    }

    @Override
    public void getChunksClose() {
        /*
         * close RS/PS
         * set all batch* vars to -1/null
         */
        closeSilent(batchResultSet);
        closeSilent(batchPreparedStatement);
        batchResultSet = null;
        batchPreparedStatement = null;
        batchColCount = -1;
        batchBatchSize = -1;
        batchRowCount = -1;
        batchResultSetDone = null;
        batchSelectStmt = null;
        // batchResultTypes = null; can be requested from outside, after finishing
    }
    //DEV

    private void setConnection(Connection con) throws SQLException {
        this.con = con;
        if (con == null)
            return;
        String prodName = con.getMetaData().getDatabaseProductName();
        // TODO MAP ORACLE
        if ("PostgreSQL".equals(prodName))
            dbProduct = DBProduct.POSTGRESQL;
        else if ("Microsoft SQL Server".equals(prodName))
            dbProduct = DBProduct.MSSQLSERVER;
        else if ("Oracle".equals(prodName))
            dbProduct = DBProduct.ORACLE;
        else if ("MySQL".equals(prodName))
            dbProduct = DBProduct.MYSQL;
        else {
            info("unmapped dbProduct=" + prodName);
            dbProduct = DBProduct.GENERIC;
        }
    }

    public Calendar getCalendar() {
        return calendar;
    }

    @Override
    public void closeConnection() {
        try {
            // close alle dependent caches
            if (lruStatementCache != null)
                lruStatementCache.clear();
            Connection c = getConnection();
            if (c != null && !c.isClosed())
                c.close();
            setConnection(null);
        } catch (SQLException ignore) {
            error("cannot close connection", ignore);
        }
    }

    @Override
    public Connection getConnection() {
        return con;
    }

    private void setExpectations(Integer expectedRows, Integer expectedColumns) {
        this.expectedRows = expectedRows;
        this.expectedColumns = expectedColumns;
    }

    @Override
    public void commit() throws SQLException {
        if (getConnection().getAutoCommit() == false)
            con.commit();
    }

    @Override
    public void commitSilent() {
        try {
            commit();
        } catch (SQLException ignore) {
            error("cannot commit. Connection=" + getConnection(), ignore);
        }
    }

    @Override
    public void rollback() throws SQLException {
        if (getConnection().getAutoCommit() == false)
            con.rollback();
    }

    @Override
    public void rollbackSilent() {
        try {
            rollback();
        } catch (SQLException ignore) {
            error("cannot rollback. Connection=" + getConnection(), ignore);
        }
    }

    @Override
    public boolean isPreparable(String sqlStmt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCalendar(Calendar c) {
        calendar = c;
    }

    @Override
    public ResultSetMetaData getPreviousMetaData() {
        return lastMetaData;
    }

    @Override
    public int[] getPreviousRowSQLTypes() {
        return batchResultTypes == null ? null : batchResultTypes.clone();
    }

    @Override
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public void executeDDL(String ddlStmt) throws SQLException {
    	long t0 = System.currentTimeMillis();
        getAudit().startNewAuditRecord(ddlStmt);
        PreparedStatement ps = null;
        try {
            ps = getConnection().prepareStatement(ddlStmt);
            ps.execute();
        } catch (SQLException sqle) {
            printError(System.out, sqle, ddlStmt, null, null);
            getAudit().endAuditRecord(sqle);
            throw sqle;
        } finally {
            closeSilent(ps);
            getAudit().endAuditRecord();
            calculateExecTimeMs(t0);
        }
    }

    @Override
    public void executeDDLSilent(String ddlStmt) {
        try {
            executeDDL(ddlStmt);
        } catch (SQLException ignore) {
        }
    }

    @Override
    public void closeSilent(ResultSet rs) {
        if (rs == null)
            return;
        try {
            rs.close();
        } catch (SQLException ignore) {
            error("cannot close ResultSet" + rs, ignore);
        }
    }

    @Override
    public void closeSilent(Statement stmt) {
        if (stmt == null)
            return;
        try {
            stmt.close();
        } catch (SQLException ignore) {
            error("cannot close Statement" + stmt, ignore);
        }
    }

    @Override
    public int[] executeDMLBatch(String dmlStmt, List<Object[]> batchValues, int[] bindTypes)
            throws SQLException {
    	long t0 = System.currentTimeMillis();
        getAudit().startNewAuditRecord(dmlStmt);
        PreparedStatement ps = null;
        int[] affectedRows = null;
        try {
            ps = getPreparedStatement(dmlStmt);
            for (Object[] data : batchValues) {
                BindHelper.bindVariables(ps, data, bindTypes, getCalendar(), getDBProduct());
                ps.addBatch();
            }
            affectedRows = ps.executeBatch();
            return affectedRows;
        } catch (SQLException sqle) {
            printError(System.out, sqle, dmlStmt, null, bindTypes);
            getAudit().endAuditRecord(sqle);
            throw sqle;
        } finally {
            if (lruStatementCache == null)
                closeSilent(ps);
            if (getAudit().isEnabled()) {
                int rows = 0;
                if (affectedRows!=null)
	                for (int i = 0; i < affectedRows.length; i++)
	                    rows += affectedRows[i];
                getAudit().endAuditRecord(rows);
            }
            calculateExecTimeMs(t0);
        }
    }

    @Override
    public int[] executeDMLBatch(String dmlStmt, Row[] rows, int[] bindTypes) throws SQLException {
    	long t0 = System.currentTimeMillis();
        getAudit().startNewAuditRecord(dmlStmt);
        PreparedStatement ps = null;
        int[] affectedRows = null;
        try {
            ps = getPreparedStatement(dmlStmt);
            for (Row row : rows) {
                BindHelper.bindVariables(ps, row.getData(), bindTypes, getCalendar(), getDBProduct());
                ps.addBatch();
            }
            affectedRows = ps.executeBatch();
            return affectedRows;
        } catch (SQLException sqle) {
            printError(System.out, sqle, dmlStmt, null, bindTypes);
            getAudit().endAuditRecord(sqle);
            throw sqle;
        } finally {
            if (lruStatementCache == null)
                closeSilent(ps);
            if (getAudit().isEnabled()) {
                int r = 0;
                for (int i = 0; i < affectedRows.length; i++)
                    r += affectedRows[i];
                getAudit().endAuditRecord(r);
            }
            calculateExecTimeMs(t0);
        }
    }

    public void expectXRows(Row[] rows, int x) throws SQLException {
        // if negative, 0..* rows are ok, so everything is ok
        if (x<0) return;
        if (rows==null && x==0) return;

        boolean ok = true;
        if (rows == null || rows.length == 0)
            if (x == 0)
                return;
            else
                ok = false;
        if (ok) {
            if (x == rows.length)
                return;
            else
                ok = false;
        }

        if (!ok)
            throw new SQLException("expected rows: " + x + ", rows given:"
                    + (rows == null ? 0 : rows.length));
    }

    // executeCall
    @Override
    public void executeSP(String call) throws SQLException {
        executeSP(call, null, null);
    }

    @Override
    public void executeSPVarArgs(String call, Object... bindVariables) throws SQLException {
        executeSP(call, bindVariables, null);
    }

    @Override
    public void executeSP(String call, Object[] bindVariables) throws SQLException {
        executeSP(call, bindVariables, null);
    }

    @Override
    public void executeSP(String call, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
    	long t0 = System.currentTimeMillis();
        getAudit().startNewAuditRecord(call, bindVariables);
        CallableStatement cs = null;
        try {
            cs = getConnection().prepareCall(call);
            BindHelper.bindVariables(cs, bindVariables, bindTypes, getCalendar(), getDBProduct());
            cs.execute();
            cs.close();
        } catch (SQLException sqle) {
            printError(System.out, sqle, call, bindVariables, bindTypes);
            getAudit().endAuditRecord(sqle);
            throw sqle;
        } finally {
            closeSilent(cs);
            getAudit().endAuditRecord();
        }
        calculateExecTimeMs(t0);
    }

    // executeDML
    //
    @Override
    public Integer executeDML(String dmlStmt) throws SQLException {
        return executeDML(dmlStmt, null, null);
    }

    @Override
    public Integer executeDMLVarArgs(String dmlStmt, Object... bindVariables) throws SQLException {
        return executeDML(dmlStmt, bindVariables, null);
    }

    @Override
    public Integer executeDML(String dmlStmt, Object[] bindVariables) throws SQLException {
        return executeDML(dmlStmt, bindVariables, null);
    }

    @Override
    public Integer executeDML(String dmlStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
    	long t0 = System.currentTimeMillis();
        getAudit().startNewAuditRecord(dmlStmt, bindVariables);
        PreparedStatement ps = null;
        int rows = -1;
        try {
            ps = getPreparedStatement(dmlStmt);
            BindHelper.bindVariables(ps, bindVariables, bindTypes, getCalendar(), getDBProduct());
            rows = ps.executeUpdate();
            return rows;
        } catch (SQLException sqle) {
            printError(System.out, sqle, dmlStmt, bindVariables, bindTypes);
            getAudit().endAuditRecord(sqle);
            throw sqle;
        } finally {
            if (lruStatementCache == null)
                closeSilent(ps);
            getAudit().endAuditRecord(rows);
            calculateExecTimeMs(t0);
        }
    }
    //
    // executeDML

    // getRows
    //
    @Override
    public Row[] getRowsVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
        return getRows(selectStmt, bindVariables, null);
    }

    @Override
    public Row[] getRows(String selectStmt) throws SQLException {
        return getRows(selectStmt, null, null);
    }

    @Override
    public Row[] getRows(String selectStmt, Object[] bindVariables) throws SQLException {
        return getRows(selectStmt, bindVariables, null);
    }

    // getRow
    //
    @Override
    public Row getRowVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
        return getRow(selectStmt, bindVariables, null);
    }

    @Override
    public Row getRow(String selectStmt) throws SQLException {
        return getRow(selectStmt, null, null);
    }

    @Override
    public Row getRow(String selectStmt, Object[] bindVariables) throws SQLException {
        return getRow(selectStmt, bindVariables, null);
    }

    /*
    @Override
     why columnMap here?
    public Row getRow(Hashtable<String, Integer> columnMap, String selectStmt,
            Object[] bindVariables, int[] bindTypes) throws SQLException {
        Row[] rows = getRows(columnMap, selectStmt, bindVariables, bindTypes);
        expectXRows(rows, 1);
        return rows[0];
    }
    */

    @Override
    public Row getRow(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
        Row[] rows = getRows(selectStmt, bindVariables, bindTypes);
        expectXRows(rows, 1);
        return rows[0];
    }


    private Object[] convertResultRow2ObjectArray(ResultSet rs, int colCount, int rowCount,
            final int[] resultTypes, final String selectStmt) throws SQLException {
        Object[] row = new Object[colCount];
        Object v = null;
        for (int i = 1, n = 0; i <= colCount; i++, n++) {
            switch (resultTypes[n]) {
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
                v = getString(rs, i);
                break;
            case Types.SMALLINT:
                v = getInteger(rs, i);
                break;
            case Types.INTEGER:
            case Types.BIGINT:
                // FIXME: ? does bigint always match to getLong?
                v = getLong(rs, i);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                v = getBigDecimal(rs, i);
                break;
            case Types.DATE:
                v = getDate(rs, i);
                break;
            case Types.REAL:
                v = getFloat(rs, i);
                break;
            case Types.DOUBLE:
                v = getDouble(rs, i);
                break;
            case Types.TIMESTAMP:
                v = getTimestamp(rs, i);
                break;
            case Types.TIME:
                v = getTime(rs, i);
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                v = getBoolean(rs, i);
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                v = getByteArray(rs, i);
                break;
            default:
                // all special or dependent values....
                boolean err = false;
                if (getDBProduct() == DBProduct.POSTGRESQL) {
                    // numbers are hardcoded, so no postgres driver is referenced.
                    switch (resultTypes[n]) {
                    case 8: //float8
                        v = getDouble(rs, i);
                        break;
                    case -5: // smallint
                        v = getLong(rs, i);
                        break;
                    case 1111: // json
                        v = getString(rs, i);
                        break;
                    default:
                        err = true;
                    }
                } else if (getDBProduct() == DBProduct.MSSQLSERVER) {
                    switch (resultTypes[n]) {
                    case -155: // DATETIMEOFFSET
                        v = getTimestamp(rs, i);
                        break;
                    default:
                        err = true;
                    }
                } else if (getDBProduct() == DBProduct.ORACLE) {
                    switch (resultTypes[n]) {
                    case 101: // DOUBLE?
                        v = getDouble(rs, i);
                        break;
                    case -101: // Timestamp with Time Zone
                        v = getTimestamp(rs, i);
                        break;
                    case 2005: // clob
                        v = getString(rs, i);
                        break;
                    default:
                        err = true;
                    }
                } else {
                    err = true;
                }
                if (err) {
                    throw new UnsupportedOperationException(
                            "Cannot convert db-value to Java, column #" + i + " in sql <<"
                                    + selectStmt + ">>, value of resultTypes[" + n + "]="
                                    + resultTypes[n]);
                }
            }
            row[n] = v;
            if (expectedRows != null && rowCount > expectedRows) {
                throw new IllegalStateException(
                        "got more than expected rows(expectaion: " + expectedRows + ")");
            }
        }
        return row;
    }

    @Override
    public Row[] getRows(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
    	long t0 = System.currentTimeMillis();
        getAudit().startNewAuditRecord(selectStmt, bindVariables);
        PreparedStatement ps = null;
        ResultSet rs = null;
        ArrayList<Object[]> list = new ArrayList<>();
        Hashtable<String, Integer> columnMap = new Hashtable<>();
        int rowCount = 0;
        try {
            // prepare, bind and execute
            ps = getConnection().prepareStatement(selectStmt);
            BindHelper.bindVariables(ps, bindVariables, bindTypes, getCalendar(), getDBProduct());
            ps.setFetchSize(fetchSize);
            rs = ps.executeQuery();
            lastMetaData = rs.getMetaData();
            rs.setFetchSize(fetchSize);

            // parse types of resultset
            int colCount = rs.getMetaData().getColumnCount();
            int[] resultTypes = new int[colCount];
            for (int i = 1, n = 0; i <= colCount; i++, n++) {
                resultTypes[n] = rs.getMetaData().getColumnType(i);
                //System.out.println(n+":"+resultTypes[n]+":"+lastMetaData.getColumnClassName(i)+" "+lastMetaData.getColumnTypeName(i));
                columnMap.put(rs.getMetaData().getColumnLabel(i), n);
            }
            if (expectedColumns != null && expectedColumns != colCount) {
                throw new IllegalStateException(
                        "Expected columns: " + expectedColumns + ", columns selected: " + colCount);
            }

            // fetch data
            while (rs.next()) {
                rowCount++;
                Object[] row = convertResultRow2ObjectArray(rs, colCount, rowCount, resultTypes,
                        selectStmt);
                list.add(row);
            }

        } catch (SQLException sqle) {
            printError(System.out, sqle, selectStmt, bindVariables, bindTypes);
            getAudit().endAuditRecord(sqle);
            throw sqle;
        } finally {
            closeSilent(rs);
            if (lruStatementCache == null)
                closeSilent(ps);
            setExpectations(null, null); // reset expectations
            getAudit().endAuditRecord(rowCount);
        }

        Row[] rows = new Row[list.size()];
        int i = 0;
        for (Object[] rowData : list)
            rows[i++] = new Row(rowData, columnMap);
        calculateExecTimeMs(t0);
        return rows;
    }

    //
    // getRows
    private enum ReturnType {
        STRING, DOUBLE, LONG, BIGDECIMAL, SQLDATE, SQLTIMESTAMP, BYTEARRAY
    }

    private Object getObject(String selectStmt, ReturnType rt) throws SQLException {
        long t0 = System.currentTimeMillis();
        getAudit().startNewAuditRecord(selectStmt);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = getPreparedStatement(selectStmt);
            rs = ps.executeQuery();
            rs.next();
            Object v = null;
            switch (rt) {
            case STRING:
                v = rs.getString(1);
                break;
            case LONG:
                v = Long.valueOf(rs.getLong(1));
                break;
            case BIGDECIMAL:
                v = rs.getBigDecimal(1);
                break;
            case SQLDATE:
                v = rs.getDate(1);
                break;
            case SQLTIMESTAMP:
                v = rs.getTimestamp(1);
                break;
            case DOUBLE:
                v = rs.getDouble(1);
                break;
            case BYTEARRAY:
                v = rs.getBytes(1);
                break;
            }
            if (rs.wasNull())
                return null;
            if (rs.next())
                throw new SQLException("single row query returned more than one row");
            return v;
        } catch (SQLException sqle) {
            printError(System.out, sqle, selectStmt, null, null);
            getAudit().endAuditRecord(sqle);
            throw sqle;
        } finally {
            closeSilent(rs);
            if (lruStatementCache == null)
                closeSilent(ps);
            getAudit().endAuditRecord(1);
            calculateExecTimeMs(t0);
        }
    }

    // <T> get<T>(selectStmt)
    //
    @Override
    public String getString(String selectStmt) throws SQLException {
        return (String) getObject(selectStmt, ReturnType.STRING);
    }

    @Override
    public Long getLong(String selectStmt) throws SQLException {
        return (Long) getObject(selectStmt, ReturnType.LONG);
    }

    @Override
    public Double getDouble(String selectStmt) throws SQLException {
        return (Double) getObject(selectStmt, ReturnType.DOUBLE);
    }

    @Override
    public BigDecimal getBigDecimal(String selectStmt) throws SQLException {
        return (BigDecimal) getObject(selectStmt, ReturnType.BIGDECIMAL);
    }

    @Override
    public java.sql.Timestamp getTimestamp(String selectStmt) throws SQLException {
        return (java.sql.Timestamp) getObject(selectStmt, ReturnType.SQLTIMESTAMP);
    }

    @Override
    public byte[] getRaw(String selectStmt) throws SQLException {
        return (byte[]) getObject(selectStmt, ReturnType.BYTEARRAY);
    }
    //
    // <T> get<T>(selectStmt)

    // <T> get<T>VarArgs(selectStmt, bindVars...)
    //
    @Override
    public String getStringVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
        return (String) getObject(selectStmt, bindVariables);
    }

    @Override
    public Long getLongVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
        return ConversionHelper.toLong(getObject(selectStmt, bindVariables));
    }

    @Override
    public BigDecimal getBigDecimalVarArgs(String selectStmt, Object... bindVariables)
            throws SQLException {
        return ConversionHelper.toBigDecimal(getObject(selectStmt, bindVariables));
    }

    @Override
    public Double getDoubleVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
        return (Double) getObject(selectStmt, bindVariables);
    }

    @Override
    public Timestamp getTimestampVarArgs(String selectStmt, Object... bindVariables)
            throws SQLException {
        return (Timestamp) getObject(selectStmt, bindVariables);
    }

    @Override
    public byte[] getRawVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
        return (byte[]) getObject(selectStmt, bindVariables);
    }
    //
    // <T> get<T>VarArgs(selectStmt, bindVars...)

    // <T> get<T>(selectStmt, bindVars)
    //
    @Override
    public String getString(String selectStmt, Object[] bindVariables) throws SQLException {
        return (String) getObject(selectStmt, bindVariables);
    }

    @Override
    public Long getLong(String selectStmt, Object[] bindVariables) throws SQLException {
        return ConversionHelper.toLong(getObject(selectStmt, bindVariables));
    }

    @Override
    public Timestamp getTimestamp(String selectStmt, Object[] bindVariables) throws SQLException {
        return (Timestamp) getObject(selectStmt, bindVariables);
    }

    @Override
    public BigDecimal getBigDecimal(String selectStmt, Object[] bindVariables) throws SQLException {
        return ConversionHelper.toBigDecimal(getObject(selectStmt, bindVariables));
    }

    @Override
    public Double getDouble(String selectStmt, Object[] bindVariables) throws SQLException {
        return (Double) getObject(selectStmt, bindVariables);
    }

    @Override
    public byte[] getRaw(String selectStmt, Object[] bindVariables) throws SQLException {
        return (byte[]) getObject(selectStmt, bindVariables);
    }

    private Object getObject(String selectStmt, Object[] bindVariables) throws SQLException {
        return getObject(selectStmt, bindVariables, null);
    }
    //
    // <T> get<T>(selectStmt, bindVars)

    // <T> get<T>(selectStmt, bindVars, bindTypes)
    //
    @Override
    public String getString(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        return (String) getObject(selectStmt, bindVariables, bindTypes);
    }

    @Override
    public Long getLong(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        return ConversionHelper.toLong(getObject(selectStmt, bindVariables, bindTypes));
    }

    @Override
    public Timestamp getTimestamp(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        return (Timestamp) getObject(selectStmt, bindVariables, bindTypes);
    }

    @Override
    public Double getDouble(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        return (Double) getObject(selectStmt, bindVariables, bindTypes);
    }

    @Override
    public BigDecimal getBigDecimal(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        return ConversionHelper.toBigDecimal(getObject(selectStmt, bindVariables, bindTypes));
    }

    @Override
    public byte[] getRaw(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        return (byte[]) getObject(selectStmt, bindVariables, bindTypes);
    }

    private Object getObject(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        setExpectations(1, 1);
        Row[] r = getRows(selectStmt, bindVariables, bindTypes);
        expectXRows(r, 1);
        return r[0].get(0);
    }

    // List<<T>> get<T>s(selectStmt)
    //
    @Override
    public String[] getStrings(String selectStmt) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt);
        return ConversionHelper.toStrings(in);
    }

    @Override
    public Long[] getLongs(String selectStmt) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt);
        return ConversionHelper.toLongs(in);
    }

    @Override
    public Double[] getDoubles(String selectStmt) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt);
        return ConversionHelper.toDoubles(in);
    }

    @Override
    public BigDecimal[] getBigDecimals(String selectStmt) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt);
        return ConversionHelper.toBigDecimals(in);
    }

    @Override
    public java.sql.Timestamp[] getTimestamps(String selectStmt) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt);
        return ConversionHelper.toTimestamps(in);
    }

    @Override
    public byte[][] getRaws(String selectStmt) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt);
        return ConversionHelper.toRaws(in);
    }
    //
    // List<<T>> get<T>s(selectStmt)

    // List<<T>> get<T>VarArgs(selectStmt, bindVars...)
    //
    @Override
    public String[] getStringsVarArgs(String selectStmt, Object... bindVariables)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRowsVarArgs(selectStmt, bindVariables);
        return ConversionHelper.toStrings(in);
    }

    @Override
    public Long[] getLongsVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRowsVarArgs(selectStmt, bindVariables);
        return ConversionHelper.toLongs(in);
    }

    @Override
    public Double[] getDoublesVarArgs(String selectStmt, Object... bindVariables)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRowsVarArgs(selectStmt, bindVariables);
        return ConversionHelper.toDoubles(in);
    }

    @Override
    public BigDecimal[] getBigDecimalsVarArgs(String selectStmt, Object... bindVariables)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRowsVarArgs(selectStmt, bindVariables);
        return ConversionHelper.toBigDecimals(in);
    }

    @Override
    public java.sql.Timestamp[] getTimestampsVarArgs(String selectStmt, Object... bindVariables)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRowsVarArgs(selectStmt, bindVariables);
        return ConversionHelper.toTimestamps(in);
    }

    @Override
    public byte[][] getRawsVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRowsVarArgs(selectStmt, bindVariables);
        return ConversionHelper.toRaws(in);
    }
    //
    // List<<T>> get<T>VarArgs(selectStmt, bindVars...)

    // List<<T>> get<T>s(selectStmt, bindVars, bindTypes)
    //
    @Override
    public String[] getStrings(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, bindTypes);
        return ConversionHelper.toStrings(in);
    }

    @Override
    public Long[] getLongs(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, bindTypes);
        return ConversionHelper.toLongs(in);
    }

    @Override
    public Double[] getDoubles(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, bindTypes);
        return ConversionHelper.toDoubles(in);
    }

    @Override
    public BigDecimal[] getBigDecimals(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, bindTypes);
        return ConversionHelper.toBigDecimals(in);
    }

    @Override
    public java.sql.Timestamp[] getTimestamps(String selectStmt, Object[] bindVariables,
            int[] bindTypes) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, bindTypes);
        return ConversionHelper.toTimestamps(in);
    }

    @Override
    public byte[][] getRaws(String selectStmt, Object[] bindVariables, int[] bindTypes)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, bindTypes);
        return ConversionHelper.toRaws(in);
    }
    //
    // List<<T>> get<T>s(selectStmt, bindVars, bindTypes)

    // List<<T>> get<T>s(selectStmt, bindVars)
    //
    @Override
    public String[] getStrings(String selectStmt, Object[] bindVariables) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables);
        return ConversionHelper.toStrings(in);
    }

    @Override
    public Long[] getLongs(String selectStmt, Object[] bindVariables) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, null);
        return ConversionHelper.toLongs(in);
    }

    @Override
    public Double[] getDoubles(String selectStmt, Object[] bindVariables) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, null);
        return ConversionHelper.toDoubles(in);
    }

    @Override
    public BigDecimal[] getBigDecimals(String selectStmt, Object[] bindVariables)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, null);
        return ConversionHelper.toBigDecimals(in);
    }

    @Override
    public java.sql.Timestamp[] getTimestamps(String selectStmt, Object[] bindVariables)
            throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, null);
        return ConversionHelper.toTimestamps(in);
    }

    @Override
    public byte[][] getRaws(String selectStmt, Object[] bindVariables) throws SQLException {
        setExpectations(null, 1);
        Row[] in = getRows(selectStmt, bindVariables, null);
        return ConversionHelper.toRaws(in);
    }

    // INTERNAL HELPER
    //
    private static String getString(ResultSet rs, int pos) throws SQLException {
        return rs.getString(pos);
    }

    @SuppressWarnings("unused")
    // nevertheless, this is here to be complete
    private static String getString(ResultSet rs, String col) throws SQLException {
        return rs.getString(col);
    }

    private static Long getLong(ResultSet rs, int pos) throws SQLException {
        long v = rs.getLong(pos);
        if (rs.wasNull())
            return null;
        return v;
    }

    @SuppressWarnings("unused")
    // nevertheless, this is here to be complete
    private static Long getLong(ResultSet rs, String col) throws SQLException {
        long v = rs.getLong(col);
        if (rs.wasNull())
            return null;
        return v;
    }

    private static Integer getInteger(ResultSet rs, int pos) throws SQLException {
        int v = rs.getInt(pos);
        if (rs.wasNull())
            return null;
        return v;
    }

    private static Boolean getBoolean(ResultSet rs, int pos) throws SQLException {
        boolean v = rs.getBoolean(pos);
        if (rs.wasNull())
            return null;
        return v;
    }

    private static Float getFloat(ResultSet rs, int pos) throws SQLException {
        float v = rs.getFloat(pos);
        if (rs.wasNull())
            return null;
        return v;
    }

    private static Double getDouble(ResultSet rs, int pos) throws SQLException {
        double v = rs.getDouble(pos);
        if (rs.wasNull())
            return null;
        return v;
    }

    private static BigDecimal getBigDecimal(ResultSet rs, int pos) throws SQLException {
        return rs.getBigDecimal(pos);
    }

    private static java.sql.Date getDate(ResultSet rs, int pos) throws SQLException {
        return rs.getDate(pos);
    }

    private static java.sql.Timestamp getTimestamp(ResultSet rs, int pos) throws SQLException {
        return rs.getTimestamp(pos);
    }

    private static java.sql.Time getTime(ResultSet rs, int pos) throws SQLException {
        return rs.getTime(pos);
    }

    private static byte[] getByteArray(ResultSet rs, int i) throws SQLException {
        return rs.getBytes(i);
    }

    private static void printError(PrintStream out, SQLException sqle, String stmt,
            Object[] bindVariables, int[] bindTypes) {
        out.println(sqle.getMessage());
        out.println("sql=" + stmt);
        if (bindVariables == null)
            out.println("bindVariables=NULL");
        else {
            StringBuffer sb = new StringBuffer("bindVariables, len=");
            sb.append(bindVariables.length);
            sb.append(", values=");
            for (int i = 0; i < bindVariables.length - 1; i++)
                sb.append('[').append(i).append("]=").append(bindVariables[i]).append(';');
            sb.append('[').append(bindVariables.length - 1).append("]=")
                    .append(bindVariables[bindVariables.length - 1]);
            out.println(sb.toString());
        }
        // todo: print constant names instead of values
        if (bindTypes == null)
            out.println("bindTypes=NULL");
        else {
            StringBuffer sb = new StringBuffer("bindTypes, len=");
            sb.append(bindTypes.length);
            sb.append(", values=");
            for (int i = 0; i < bindTypes.length - 1; i++)
                sb.append('[').append(i).append("]=").append(bindTypes[i]).append(';');
            sb.append('[').append(bindTypes.length - 1).append("]=")
                    .append(bindTypes[bindTypes.length - 1]);
            out.println(sb.toString());
        }
    }

}
