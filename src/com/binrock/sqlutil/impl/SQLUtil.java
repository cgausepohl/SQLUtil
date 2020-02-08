package com.binrock.sqlutil.impl;

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

import com.binrock.sqlutil.Row;
import com.binrock.sqlutil.SQLUtilInterface;
import com.binrock.sqlutil.exception.ReturnedMoreThanOneRowException;
import com.binrock.sqlutil.exception.ReturnedNoRowException;
import com.binrock.sqlutil.exception.SingleRowQueryException;

public class SQLUtil implements SQLUtilInterface {

	private Connection con;
	//STATprivate Hashtable<String, ArrayList<ExecutionStatistics>> statisticsExecutionTiming = new Hashtable<>();
	//STATprivate boolean doStatistics = false;
	private Calendar calendar;
	private ResultSetMetaData lastMetaData;
	private Integer expectedRows, expectedColumns;
	private int fetchSize=0;
	private Hashtable<String, Integer> currentIterationColumnMap;
	
	// batch
	private ResultSet batchResultSet;
	private PreparedStatement batchPreparedStatement;
	private int batchColCount, batchBatchSize, batchRowCount;
	private Boolean batchResultSetDone;
	private String batchSelectStmt;
	private int[] batchResultTypes;
	
	enum DBProducts {POSTGRESQL, MYSQL, ORACLE, GENERIC};
	private DBProducts dbProduct = DBProducts.GENERIC;

	public SQLUtil(Connection con) throws SQLException {
		setConnection(con);
	}

//DEV
	@Override
	public void prepareBatchMode(String selectStmt, int batchSize) throws SQLException {
		/* prepare PS+RS
		 * metaData
		 * colcount
		 * typeMap?
		 */
				
		// prepare, bind and execute
		batchPreparedStatement = getConnection().prepareStatement(selectStmt);
//FIXME?		batchPreparedStatement = BindHelper.bindVariables(batchPreparedStatement, null, null, getCalendar());
		batchPreparedStatement.setFetchSize(fetchSize);
		batchResultSet = batchPreparedStatement.executeQuery();
		batchResultSet.setFetchSize(fetchSize);
		
		lastMetaData = batchResultSet.getMetaData();
		batchColCount = lastMetaData.getColumnCount();
		batchResultTypes = new int[batchColCount];
		for (int i=1,n=0; i<=batchColCount; i++, n++) {
			batchResultTypes[n] = lastMetaData.getColumnType(i);
			// a little hack. SQL.INTEGER are mapped to java long. Java long must be mapped to SQL.BIGINT
			if (batchResultTypes[n]==Types.INTEGER)
				batchResultTypes[n] = Types.BIGINT;
//			if (currentIterationColumnMap!=null) 
//				currentIterationColumnMap.put(batchResultSet.getMetaData().getColumnLabel(i), n);
		}
		if (expectedColumns!=null && expectedColumns!=batchColCount) {
			throw new IllegalStateException("Expected columns: "+expectedColumns+", columns selected: "+batchColCount);
		}

		batchBatchSize = batchSize;
		batchRowCount = 0;
		batchResultSetDone = false;
		batchSelectStmt = selectStmt;
	}
	
	@Override
	public Row[] getRowsBatch() throws SQLException {

		if (batchResultSet==null || batchResultSet.isClosed())
			throw new SQLException("current iteration not prepared (no call of prepareRowsIterated)");
		if (batchResultSetDone) return null;
		
		int currBatchCounter=0;
		ArrayList<Row> rowsList = new ArrayList<>(batchBatchSize);
		while (true) {
			if (batchResultSet.next()) {
				Object[] row = convertResultRow2ObjectArray(
						batchResultSet, batchColCount, batchRowCount, batchResultTypes, batchSelectStmt);
				rowsList.add(new Row(row, /*columnMap*/null));
				batchRowCount++;
				currBatchCounter++;
				if (currBatchCounter>=batchBatchSize) 
					break;
			} else {
				batchResultSetDone = true;
				break;
			}
		}
		
		if (rowsList.size()==0) return null;
		
		Row[] rows = new Row[rowsList.size()];
		int i=0;
		for (Row row: rowsList) {
			rows[i] = row;
			i++;
		}
		return rows;
	}
	
	@Override
	public void closeBatchMode() {
		/*
		 * close RS/PS
		 * set all batch* vars to -1/null
		 */
		closeSilent(batchResultSet);
		closeSilent(batchPreparedStatement);
		if (currentIterationColumnMap!=null) currentIterationColumnMap.clear();  // is this necessary/useful?
		batchResultSet = null;
		batchPreparedStatement = null;
		batchColCount=-1;
		batchBatchSize=-1;
		batchRowCount=-1;
		batchResultSetDone = null;
		batchSelectStmt = null;
		// batchResultTypes = null; can be requested from outside, after finishing
	}
//DEV
	
	
	
	
	
	private void setConnection(Connection con) throws SQLException {
		this.con = con;
		if (con==null) return;
		String prodName = con.getMetaData().getDatabaseProductName();
		if ("PostgreSQL".equals(prodName))
			dbProduct = DBProducts.POSTGRESQL;
		else
			dbProduct = DBProducts.GENERIC;
	}
	
	public Calendar getCalendar() {
		return calendar;
	}

	@Override
	public void closeConnection() {
		//startExecutionTiming("createSQLUtil");
		try {
			Connection c = getConnection();
			if (c != null && !c.isClosed())
				c.close();
			setConnection(null);
		} catch (SQLException ignore) {
			//endExecutionTiming(sqle);
		}
		//endExecutionTiming();
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
		if (getConnection().getAutoCommit()==false) con.commit();
	}

	@Override
	public void commitSilent()  {
		try {
			commit();
		} catch (SQLException ignore) {}
	}

	@Override
	public void rollback() throws SQLException {
		if (getConnection().getAutoCommit()==false) con.rollback();
	}

	@Override
	public void rollbackSilent() {
		try {
			rollback();
		} catch (SQLException ignore) {}
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
	public ResultSetMetaData getLastMetaData() {
		return lastMetaData;
	}
	
	@Override
	public int[] getLastRowSQLTypes() {
		return batchResultTypes;
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
		//STATExecutionStatistics stat = null;//createExecStats("execute:"+ddlStmt);
		PreparedStatement ps = null;
		try {
			ps = getConnection().prepareStatement(ddlStmt);
			ps.execute();
			//STAT			if (stat!=null) stat.setResultOK();
		} catch (SQLException sqle) {
			//STATif (stat!=null) stat.setResultError(sqle.getMessage());
			throw sqle;
		} finally {
			closeSilent(ps);
		}
	}

	
	@Override
	public void closeSilent(ResultSet rs) {
		try {
			if (rs != null)
				rs.close();
		} catch (SQLException ignore) {
		}
	}
	
	@Override
	public void closeSilent(Statement stmt) {
		try {
			if (stmt != null)
				stmt.close();
		} catch (SQLException ignore) {
		}
	}

	
	@Override
	public int[] executeDMLBatch(String dmlStmt, List<Object[]> batchValues, int[] bindTypes) throws SQLException {
		PreparedStatement ps = null;
		try {
			ps = getConnection().prepareStatement(dmlStmt);
			for (Object[] data: batchValues) {
				BindHelper.bindVariables(ps, data, bindTypes, getCalendar());
				ps.addBatch();
			}
			int[] affectedRows = ps.executeBatch();
			return affectedRows;
		} finally {
			closeSilent(ps);
		}
	}

	@Override
	public int[] executeDMLBatch(String dmlStmt, Row[] rows, int[] bindTypes) throws SQLException {
		PreparedStatement ps = null;
		try {
			ps = getConnection().prepareStatement(dmlStmt);
			for (Row row: rows) {
				BindHelper.bindVariables(ps, row.getData(), bindTypes, getCalendar());
				ps.addBatch();
			}
			int[] affectedRows = ps.executeBatch();
			return affectedRows;
		} finally {
			closeSilent(ps);
		}
	}

	@Override
	public void enableTimings(boolean enable) {
		//doStatistics = enable;
	}

	@Override
	public void startExecutionTiming(String action) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void endExecutionTiming() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void endExecutionTiming(Throwable t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void flushAllTimings() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void printTimingsSummary() {
		//throw new UnsupportedOperationException();
	}

	@Override
	public void printTimingsComplete() {
		throw new UnsupportedOperationException();
	}

	private void expectOneRow(Row[] rows) throws SingleRowQueryException {
		if (rows==null) throw new IllegalArgumentException("paramater l must have a value");
		if (rows.length>1) throw new ReturnedMoreThanOneRowException();
		if (rows.length==0) throw new ReturnedNoRowException();
	}

	@Override
	public void checkExactNumberRowsExpected(Row[] rows, int expectedRows) throws SQLException  {
		boolean ok = true;
		if (rows == null || rows.length == 0)
			if (expectedRows == 0)
				return;
			else
				ok = false;

		if (expectedRows == rows.length)
			return;
		else
			ok = false;

		if (!ok)
			throw new SQLException(
					"expected rows: " + expectedRows + ", rows given:" + (rows == null ? 0 : rows.length));
	}

	// executeCall
	@Override
	public void executeSP(String call) throws SQLException {
		executeSP(call, null, null);
	}
	@Override
	public void executeSPVarArgs(String call, Object... bindVariables) throws SQLException  {
		executeSP(call,bindVariables, null);
	}
	@Override
	public void executeSP(String call, Object[] bindVariables) throws SQLException  {
		executeSP(call, bindVariables, null);
	}
	@Override
	public void executeSP(String call, Object[] bindVariables, int[] bindTypes) throws SQLException  {
		CallableStatement cs = null;
		try {
			cs = getConnection().prepareCall(call);
			BindHelper.bindVariables(cs, bindVariables, bindTypes, getCalendar());
			cs.execute();
			cs.close();			
		} catch (SQLException sqle) {
			throw sqle;
		} finally {
			closeSilent(cs);
		}
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
	public Integer executeDML(String dmlStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		PreparedStatement ps = null;
		try {
			ps = getConnection().prepareStatement(dmlStmt);
			BindHelper.bindVariables(ps, bindVariables, bindTypes, getCalendar());
			int rows = ps.executeUpdate();
			return rows;
		} catch (SQLException sqle) {
			throw sqle;
		} finally {
			closeSilent(ps);
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
	
	public Row[] getRows(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		return getRows(null, selectStmt, bindVariables, bindTypes);
	}
	
	private Object[] convertResultRow2ObjectArray(ResultSet rs, int colCount, int rowCount, final int[] resultTypes, final String selectStmt) throws SQLException {
		Object[] row = new Object[colCount];
		Object v = null;
		for (int i=1, n=0; i<=colCount; i++, n++) {
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
				v = getLong(rs, i);
				break;
			case Types.NUMERIC:
			case Types.DECIMAL:
				v = getBigDecimal(rs, i);
				break;
			case Types.REAL:
				v = getFloat(rs, i);
				break;
			case Types.DATE:
				v = getDate(rs, i);
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
				boolean err=false;
				if (dbProduct==DBProducts.POSTGRESQL) {
					switch (resultTypes[n]) {
					case 8: //float8
						v = getDouble(rs,i);
						break;
					case -5: // smallint
						v = getLong(rs, i);
						break;
					case 1111: // json
						v = getString(rs, i);
						break;
					default:
						err=true;
					}
				} else {
					err=true;
				}
				if (err) {
					throw new UnsupportedOperationException("Not matched, column #"+i+" in sql <<"+selectStmt+
							">>, value of resultTypes["+n+"]="+resultTypes[n]);
				}
			}
			row[n] = v;
			if (expectedRows!=null && rowCount>expectedRows) {
				throw new IllegalStateException("got more than expected rows(expectaion: "+expectedRows+")");
			}
		}
		return row;
	}
	
	@Override
	public Row[] getRows(Hashtable<String, Integer> columnMap, String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		ArrayList<Object[]> list = new ArrayList<>();
		try {
			// prepare, bind and execute
			ps = getConnection().prepareStatement(selectStmt);
			BindHelper.bindVariables(ps, bindVariables, bindTypes, getCalendar());
			ps.setFetchSize(fetchSize);
			rs = ps.executeQuery();
			lastMetaData = rs.getMetaData();
			rs.setFetchSize(fetchSize);
			
			// parse types of resultset
			int colCount = rs.getMetaData().getColumnCount();
			int[] resultTypes = new int[colCount];
			for (int i=1,n=0; i<=colCount; i++, n++) {
				resultTypes[n] = rs.getMetaData().getColumnType(i);
				if (columnMap!=null) 
					columnMap.put(rs.getMetaData().getColumnLabel(i), n);
			}
			if (expectedColumns!=null && expectedColumns!=colCount) {
				throw new IllegalStateException("Expected columns: "+expectedColumns+", columns selected: "+colCount);
			}
			
			// fetch data
			int rowCount = 0;
			while (rs.next()) {
				rowCount++;
				Object[] row = convertResultRow2ObjectArray(rs, colCount, rowCount, resultTypes, selectStmt);
				list.add(row);
			}
					
		} catch (SQLException sqle) {
			printError(System.out, sqle, selectStmt, bindVariables, bindTypes);
			throw sqle;
		} finally {
			closeSilent(rs);
			closeSilent(ps);
			setExpectations(null, null);  // reset expectations
		}
		
		Row[] rows = new Row[list.size()];
		int i=0;
		for (Object[] rowData: list)
			rows[i++] = new Row(rowData, columnMap);
		return rows;
	}

	//
	// getRows

	private enum ReturnType {STRING, DOUBLE, LONG, BIGDECIMAL, SQLDATE, SQLTIMESTAMP, BYTEARRAY};
	private Object getObject(String selectStmt, ReturnType rt) throws SQLException {
		//STATExecutionStatistics stat = null;//createExecStats("getValue("+rt+"):"+sql);
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = getConnection().prepareStatement(selectStmt);
			rs = ps.executeQuery();
			rs.next();
			Object v=null;
			switch (rt) {
			case STRING       : v = rs.getString(1); break;
			case LONG         : v = new Long(rs.getLong(1)); break;
			case BIGDECIMAL   : v = rs.getBigDecimal(1); break;
			case SQLDATE      : v = rs.getDate(1); break;
			case SQLTIMESTAMP : v = rs.getTimestamp(1); break;
			case DOUBLE       : v = rs.getDouble(1); break;
			case BYTEARRAY    : v = rs.getBytes(1); break;
			}
			if (rs.wasNull()) return null;
			if (rs.next())
				throw new SQLException("single row query returned more than one row");
			return v;
		} catch (SQLException sqle) {
			//STATif (stat!=null) stat.setResultError(sqle.getMessage());
			throw sqle;
		} finally {
			closeSilent(rs);
			closeSilent(ps);
			//STATif (stat!=null) stat.setResultOK(1);
		}
	}
	
	// <T> get<T>(selectStmt)
	//
	public String getString(String selectStmt) throws SQLException {
		return (String)getObject(selectStmt, ReturnType.STRING);	
	}

	public Long getLong(String selectStmt) throws SQLException {
		return (Long)getObject(selectStmt, ReturnType.LONG);
	}
	
	public Double getDouble(String selectStmt) throws SQLException {
		return (Double)getObject(selectStmt, ReturnType.DOUBLE);
	}

	public BigDecimal getBigDecimal(String selectStmt) throws SQLException {
		return (BigDecimal)getObject(selectStmt, ReturnType.BIGDECIMAL);
	}
	
	public java.sql.Timestamp getTimestamp(String selectStmt) throws SQLException {
		return (java.sql.Timestamp)getObject(selectStmt, ReturnType.SQLTIMESTAMP);
	}

	public byte[] getRaw(String selectStmt) throws SQLException {
		return (byte[])getObject(selectStmt, ReturnType.BYTEARRAY);
	}
	//
	// <T> get<T>(selectStmt)

	// <T> get<T>VarArgs(selectStmt, bindVars...)
	//
	public String getStringVarArgs(String selectStmt, Object...bindVariables) throws SQLException {
		return (String)getObject(selectStmt, bindVariables);
	}

	public Long getLongVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
		return ConversionHelper.toLong(getObject(selectStmt, bindVariables));
	}
	
	public BigDecimal getBigDecimalVarArgs(String selectStmt, Object...bindVariables) throws SQLException {
		return ConversionHelper.toBigDecimal(getObject(selectStmt, bindVariables));
	}

	public Double getDoubleVarArgs(String selectStmt, Object...bindVariables) throws SQLException {
		return (Double)getObject(selectStmt, bindVariables);
	}
	
	public Timestamp getTimestampVarArgs(String selectStmt, Object...bindVariables) throws SQLException {
		return (Timestamp)getObject(selectStmt, bindVariables);
	}
	
	public byte[] getRawVarArgs(String selectStmt, Object...bindVariables) throws SQLException {
		return (byte[])getObject(selectStmt, bindVariables);
	}
	//
	// <T> get<T>VarArgs(selectStmt, bindVars...)

	// <T> get<T>(selectStmt, bindVars)
	//
	@Override
	public String getString(String selectStmt, Object[] bindVariables) throws SQLException {
		return (String)getObject(selectStmt, bindVariables);
	}

	public Long getLong(String selectStmt, Object[] bindVariables) throws SQLException {
		return ConversionHelper.toLong(getObject(selectStmt, bindVariables));
	}
	
	public Timestamp getTimestamp(String selectStmt, Object[] bindVariables) throws SQLException {
		return (Timestamp)getObject(selectStmt, bindVariables);
	}
	
	public BigDecimal getBigDecimal(String selectStmt, Object[] bindVariables) throws SQLException {
		return ConversionHelper.toBigDecimal(getObject(selectStmt, bindVariables));
	}
	
	public Double getDouble(String selectStmt, Object[] bindVariables) throws SQLException {
		return (Double)getObject(selectStmt, bindVariables);
	}
	
	public byte[] getRaw(String selectStmt, Object[] bindVariables) throws SQLException {
		return (byte[])getObject(selectStmt, bindVariables);
	}

	private Object getObject(String selectStmt, Object[] bindVariables) throws SQLException {
		return getObject(selectStmt, bindVariables, null);
	}
	//
	// <T> get<T>(selectStmt, bindVars)

	// <T> get<T>(selectStmt, bindVars, bindTypes)
	//
	@Override
	public String getString(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		return (String)getObject(selectStmt, bindVariables, bindTypes);
	}

	public Long getLong(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		return ConversionHelper.toLong(getObject(selectStmt, bindVariables, bindTypes));
	}

	public Timestamp getTimestamp(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		return (Timestamp)getObject(selectStmt, bindVariables, bindTypes);
	}

	public Double getDouble(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		return (Double)getObject(selectStmt, bindVariables, bindTypes);
	}

	public BigDecimal getBigDecimal(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		return ConversionHelper.toBigDecimal(getObject(selectStmt, bindVariables, bindTypes));
	}

	public byte[] getRaw(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		return (byte[])getObject(selectStmt, bindVariables, bindTypes);
	}
	
	private Object getObject(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		setExpectations(1,1);
		Row[] r = getRows(selectStmt, bindVariables, bindTypes);
		expectOneRow(r);
		return r[0].get(0);
	}

	// List<<T>> get<T>s(selectStmt)
	//
	public String[] getStrings(String selectStmt) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt);
		return ConversionHelper.toStrings(in);
	}

	public Long[] getLongs(String selectStmt) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt);
		return ConversionHelper.toLongs(in);
	}

	public Double[] getDoubles(String selectStmt) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt);
		return ConversionHelper.toDoubles(in);
	}

	public BigDecimal[] getBigDecimals(String selectStmt) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt);
		return ConversionHelper.toBigDecimals(in);
	}

	public java.sql.Timestamp[] getTimestamps(String selectStmt) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt);
		return ConversionHelper.toTimestamps(in);
	}

	public byte[][] getRaws(String selectStmt) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt);
		return ConversionHelper.toRaws(in);
	}
	//
	// List<<T>> get<T>s(selectStmt)
	
	// List<<T>> get<T>VarArgs(selectStmt, bindVars...)
	//
	public String[] getStringsVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRowsVarArgs(selectStmt, bindVariables);
		return ConversionHelper.toStrings(in);
	}

	public Long[] getLongsVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRowsVarArgs(selectStmt, bindVariables);
		return ConversionHelper.toLongs(in);
	}

	public Double[] getDoublesVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRowsVarArgs(selectStmt, bindVariables);
		return ConversionHelper.toDoubles(in);
	}

	public BigDecimal[] getBigDecimalsVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRowsVarArgs(selectStmt, bindVariables);
		return ConversionHelper.toBigDecimals(in);
	}

	@Override
	public java.sql.Timestamp[] getTimestampsVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRowsVarArgs(selectStmt, bindVariables);
		return ConversionHelper.toTimestamps(in);
	}

	@Override
	public byte[][] getRawsVarArgs(String selectStmt, Object... bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRowsVarArgs(selectStmt, bindVariables);
		return ConversionHelper.toRaws(in);
	}
	//
	// List<<T>> get<T>VarArgs(selectStmt, bindVars...)

	// List<<T>> get<T>s(selectStmt, bindVars, bindTypes)
	//
	@Override
	public String[] getStrings(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables, bindTypes);
		return ConversionHelper.toStrings(in);
	}

	@Override
	public Long[] getLongs(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables, bindTypes);
		return ConversionHelper.toLongs(in);
	}

	@Override
	public Double[] getDoubles(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables, bindTypes);
		return ConversionHelper.toDoubles(in);
	}

	@Override
	public BigDecimal[] getBigDecimals(String selectStmt, Object[] bindVariables, int[] bindTypes)
			throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables, bindTypes);
		return ConversionHelper.toBigDecimals(in);
	}

	@Override
	public java.sql.Timestamp[] getTimestamps(String selectStmt, Object[] bindVariables, int[] bindTypes)
			throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables, bindTypes);
		return ConversionHelper.toTimestamps(in);
	}

	@Override
	public byte[][] getRaws(String selectStmt, Object[] bindVariables, int[] bindTypes) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables, bindTypes);
		return ConversionHelper.toRaws(in);
	}
	//
	// List<<T>> get<T>s(selectStmt, bindVars, bindTypes)

	// List<<T>> get<T>s(selectStmt, bindVars)
	//
	@Override
	public String[] getStrings(String selectStmt, Object[] bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables);
		return ConversionHelper.toStrings(in);
	}

	@Override
	public Long[] getLongs(String selectStmt, Object[] bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables, null);
		return ConversionHelper.toLongs(in);
	}

	@Override
	public Double[] getDoubles(String selectStmt, Object[] bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables, null);
		return ConversionHelper.toDoubles(in);
	}
	
	@Override
	public BigDecimal[] getBigDecimals(String selectStmt, Object[] bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables, null);
		return ConversionHelper.toBigDecimals(in);
	}

	@Override
	public java.sql.Timestamp[] getTimestamps(String selectStmt, Object[] bindVariables) throws SQLException {
		setExpectations(null,1);
		Row[] in = getRows(selectStmt, bindVariables, null);
		return ConversionHelper.toTimestamps(in);
	}

	@Override
	public byte[][] getRaws(String selectStmt, Object[] bindVariables) throws SQLException {
		setExpectations(null,1);
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
		if (rs.wasNull()) return null;
		return v;
	}

	@SuppressWarnings("unused")
	// nevertheless, this is here to be complete
	private static Long getLong(ResultSet rs, String col) throws SQLException {
		long v = rs.getLong(col);
		if (rs.wasNull()) return null;
		return v;
	}

	private static Integer getInteger(ResultSet rs, int pos) throws SQLException {
		int v = rs.getInt(pos);
		if (rs.wasNull()) return null;
		return v;
	}

	private static Boolean getBoolean(ResultSet rs, int pos) throws SQLException {
		boolean v = rs.getBoolean(pos);
		if (rs.wasNull()) return null;
		return v;
	}

	private static Float getFloat(ResultSet rs, int pos) throws SQLException {
		float v = rs.getFloat(pos);
		if (rs.wasNull()) return null;
		return v;
	}

	private static Double getDouble(ResultSet rs, int pos) throws SQLException {
		double v = rs.getDouble(pos);
		if (rs.wasNull()) return null;
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
	
	private static void printError(PrintStream out, SQLException sqle, String stmt, Object[] bindVariables, int[] bindTypes) {
		out.println(sqle.getMessage());
		out.println("sql="+stmt);
		if (bindVariables==null) 
			out.println("bindVariables=NULL");
		else {
			StringBuffer sb = new StringBuffer("bindVariables, len=");
			sb.append(bindVariables.length);
			sb.append(", values=");
			for (int i=0; i<bindVariables.length-1; i++) sb.append('[').append(i).append("]=").append(bindVariables[i]).append(';');
			sb.append('[').append(bindVariables.length-1).append("]=").append(bindVariables[bindVariables.length-1]);
			out.println(sb.toString());
		}
		// todo: print constant names instead of values
		if (bindTypes==null) 
			out.println("bindTypes=NULL");
		else {
			StringBuffer sb = new StringBuffer("bindTypes, len=");
			sb.append(bindTypes.length);
			sb.append(", values=");
			for (int i=0; i<bindTypes.length-1; i++) sb.append('[').append(i).append("]=").append(bindTypes[i]).append(';');
			sb.append('[').append(bindTypes.length-1).append("]=").append(bindTypes[bindTypes.length-1]);
			out.println(sb.toString());
		}
	}	
}





/*
package com.binrock.sqlutil;

//STATimport java.util.ArrayList;
//STATimport java.util.Hashtable;

class ExecutionStatistics {
private Long started, finished;
private Integer rowsProcessed;
private Boolean succeeded;
private String errmsg;
private Object[] bindVariables;
ExecutionStatistics(String sql, Hashtable<String, ArrayList<ExecutionStatistics>> allTimings) {
	ArrayList<ExecutionStatistics> l = allTimings.get(sql);
	if (l==null) {
		l = new ArrayList<>();
		l.add(this);
		allTimings.put(sql, l);
	} else {
		l.add(this);
	}
	this.started = System.currentTimeMillis();
}
public void setResultOK() {
	setResultOK(0);
}

public void setResultOK(Integer rowsProcessed) {
	finished = System.currentTimeMillis();
	this.rowsProcessed = rowsProcessed;
	this.succeeded = true;
}

public void setResultError(String errmsg) {
	finished = System.currentTimeMillis();
	this.rowsProcessed = null;
	this.succeeded = false;
	this.errmsg = errmsg;
}
public void setBindVariables(Object[] bindVariables) {
	this.bindVariables = bindVariables;
}
public String toString() {
	StringBuffer sb = new StringBuffer(100);
	if (succeeded)
		sb.append("OK:");
	else
		sb.append("FAIL:").append(errmsg);
	sb.append(" ms=").append(finished-started);
	sb.append("; rows=").append(rowsProcessed);
	sb.append("; started=").append(started);
	//sb.append("; finished=").append(finished);
	if (bindVariables!=null) {
		sb.append("; bindVars={");
		boolean first = true;
		for (Object o:bindVariables) {
			if (first) 
				first=false;
			else
				sb.append(';');
			sb.append(o!=null?o:"[NULL]");
		}
		sb.append('}');
	}
	return sb.toString();
}

public Long getStarted() {
	return started;
}

public Long getFinished() {
	return finished;
}

public Integer getRowsProcessed() {
	return rowsProcessed;
}

public Boolean hasSucceeded() {
	return succeeded;
}

public String getErrmsg() {
	return errmsg;
}

public Object[] getBindVariables() {
	return bindVariables;
}
}
*/

