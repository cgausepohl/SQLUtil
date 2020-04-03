package com.binrock.sqlutil.impl;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;

public final class BindHelper {

	public static PreparedStatement bindString(PreparedStatement ps, int pos, String val) throws SQLException {
		if (val != null)
			ps.setString(pos, val);
		else
			ps.setNull(pos, Types.VARCHAR);
		return ps;
	}

	public static PreparedStatement bindBigDecimal(PreparedStatement ps, int pos, BigDecimal val) throws SQLException {
		if (val != null)
			ps.setBigDecimal(pos, val);
		else
			ps.setNull(pos, Types.NUMERIC);
		return ps;
	}

	public static PreparedStatement bindInteger(PreparedStatement ps, int pos, Integer val) throws SQLException {
		if (val != null)
			ps.setInt(pos, val);
		else
			ps.setNull(pos, Types.INTEGER);
		return ps;
	}

	public static PreparedStatement bindBoolean(PreparedStatement ps, int pos, Boolean val) throws SQLException {
		if (val != null)
			ps.setBoolean(pos, val);
		else
			ps.setNull(pos, Types.BIT);
		return ps;
	}

	public static PreparedStatement bindLong(PreparedStatement ps, int pos, Long val) throws SQLException {
		if (val != null)
			ps.setLong(pos, val);
		else
			ps.setNull(pos, Types.BIGINT);
		return ps;
	}

	public static PreparedStatement bindDouble(PreparedStatement ps, int pos, Double val) throws SQLException {
		if (val != null)
			ps.setDouble(pos, val);
		else
			ps.setNull(pos, Types.DOUBLE);
		return ps;
	}

	public static PreparedStatement bindFloat(PreparedStatement ps, int pos, Float val) throws SQLException {
		if (val != null)
			ps.setFloat(pos, val);
		else
			ps.setNull(pos, Types.FLOAT);
		return ps;
	}

	public static PreparedStatement bindByteArray(PreparedStatement ps, int pos, byte[] val) throws SQLException {
		if (val != null)
			ps.setBytes(pos, val);
		else
			ps.setNull(pos, Types.VARBINARY);
		return ps;
	}

    public static PreparedStatement bindDate(PreparedStatement ps, int pos, java.util.Date val, Calendar calendar) throws SQLException {
        return bindSQLDate(ps, pos, val==null?null:new Date(val.getTime()), calendar);
    }


	public static PreparedStatement bindSQLDate(PreparedStatement ps, int pos, Date val, Calendar calendar) throws SQLException {
		if (val != null) {
			if (calendar == null)
				ps.setDate(pos, val);
			else
				ps.setDate(pos, val, calendar);
		} else
			ps.setNull(pos, Types.DATE);
		return ps;
	}

	public static PreparedStatement bindSQLTime(PreparedStatement ps, int pos, java.sql.Time val) throws SQLException {
		if (val != null)
			ps.setTime(pos, val);
		else
			ps.setNull(pos, Types.TIME);
		return ps;
	}

	public static PreparedStatement bindSQLTimestamp(PreparedStatement ps, int pos, java.sql.Timestamp val, Calendar calendar)
			throws SQLException {
		if (val != null) {
			if (calendar == null)
				ps.setTimestamp(pos, val);
			else
				ps.setTimestamp(pos, val, calendar);
		} else
			ps.setNull(pos, Types.TIMESTAMP);
		return ps;
	}

	private static int countNullElements(Object[] a) {
		if (a == null)
			return 0;
		int c = 0;
		for (int i = 0; i < a.length; i++)
			if (a[i] == null)
				c++;
		return c;
	}

	private static int[] createBindTypeArray(Object[] bindVars) throws SQLException {
		if (bindVars == null)
			return new int[0];
		int[] types = new int[bindVars.length];
		for (int i = 0; i < types.length; i++) {
			Object o = bindVars[i];
			if (o == null)
				throw new SQLException("cannot map null(bindVars["+i+"]) to a java.sql.Types");
			int t;
			if (o instanceof String) t = Types.VARCHAR;
			else if (o instanceof BigDecimal)  t=Types.NUMERIC;
			else if (o instanceof Boolean) t=Types.BIT;
			else if (o instanceof Integer) t=Types.INTEGER;
			else if (o instanceof Long) t=Types.BIGINT;
			else if (o instanceof Float) t=Types.REAL;
			else if (o instanceof Double) t=Types.DOUBLE;
			else if (o instanceof byte[]) t=Types.VARBINARY;
			else if (o instanceof Date) t=Types.DATE;
			else if (o instanceof java.sql.Time) t=Types.TIME;
			else if (o instanceof java.util.Date || o instanceof java.sql.Timestamp) t=Types.TIMESTAMP;
			else
				throw new SQLException(
						"cannot map opbject to java.sql.Types: Object class=" + o.getClass().getName());
			types[i] = t;
		}
		return types;
	}

	private static void verifyNotMatchingClassesAndBindings(Object[] vals, int[] bindTypes) {
		if (vals == null)
			return;
		if (bindTypes.length != vals.length)
			throw new IllegalArgumentException("bindVariables and bindTypes must have the same .length");
		for (int i = 0; i < vals.length; i++) {
			Object o = vals[i];
			int t = bindTypes[i];
			if (o == null)
				continue;
			if (o instanceof String) {
				if (t != Types.CHAR && t != Types.VARCHAR && t != Types.LONGVARCHAR &&
					t != Types.NCHAR && t != Types.NVARCHAR && t != Types.LONGNVARCHAR)
					throw new IllegalArgumentException(
							"String must be mapped to Types.{CHAR|VARCHAR|LONGVARCHAR} (index="+i+")");
			} else if (o instanceof BigDecimal) {
				if (t != Types.NUMERIC && t != Types.DECIMAL)
					throw new IllegalArgumentException("BigDecimal must be mapped to Types.{DECIMAL|NUMERIC} (index="+i+")");
			} else if (o instanceof Boolean) {
				if (t != Types.BIT && t != Types.BOOLEAN)
					throw new IllegalArgumentException("Boolean must be mapped to Types.{BIT|BOOLEAN} (index="+i+")") ;
			} else if (o instanceof Integer) {
				if (t != Types.TINYINT && t != Types.SMALLINT && t != Types.INTEGER)
					throw new IllegalArgumentException(
							"Integer must be mapped to Types.{TINYINT|SMALLINT|INTEGER} (index=\"+i+\")");
			} else if (o instanceof Long) {
				if (t != Types.BIGINT)
					throw new IllegalArgumentException("Long must be mapped to Types.BIGINT (index="+i+")");
			} else if (o instanceof Float) {
				if (t != Types.REAL)
					throw new IllegalArgumentException("Float must be mapped to Types.REAL (index="+i+")");
			} else if (o instanceof Double) {
				if (t != Types.FLOAT && t != Types.DOUBLE)
					throw new IllegalArgumentException("Double must be mapped to Types.{FLOAT|DOUBLE} (index="+i+")");
			} else if (o instanceof byte[]) {
				if (t != Types.BINARY && t != Types.VARBINARY && t != Types.LONGVARBINARY)
					throw new IllegalArgumentException(
							"byte[] must be mapped to Types.{BINARY|VARBINARY|LONGVARBINARY (index="+i+")");
			} else if (o instanceof Date) {
				if (t != Types.DATE)
					throw new IllegalArgumentException("Date must be mapped to Types.DATE (index="+i+")");
			} else if (o instanceof java.sql.Time) {
				if (t != Types.TIME)
					throw new IllegalArgumentException("java.sql.Time must be mapped to Types.TIME (index="+i+")");
			} else if (o instanceof java.sql.Timestamp) {
				if (t != Types.TIMESTAMP)
					throw new IllegalArgumentException("java.sql.Timestamp must be mapped to Types.TIMESTAMP (index="+i+")");
			} else {
				throw new IllegalArgumentException("bindVariables[" + i
						+ "], no mapping to java.sql.Types for JAVA-class " + o.getClass().getName() + " (index="+i+")");
			}
		}
	}

	protected static void bindVariables(PreparedStatement ps, Object[] bindVariables, int[] bindTypes, Calendar calendar) throws SQLException {
		// try to be lazy
		if (bindVariables==null || bindVariables.length==0) return;

		// check param combinations
		if (bindTypes == null && countNullElements(bindVariables) > 0)
			throw new IllegalArgumentException(
					"all elements in bindVariables must have a value (use bindTypes param to set bindVariables-elements to null)");
		// create bindTypes or check given bindTypes
		if (bindTypes == null)
			bindTypes = createBindTypeArray(bindVariables);
		else
			verifyNotMatchingClassesAndBindings(bindVariables, bindTypes);

		for (int n = 0, pos = 1; n < bindVariables.length; n++, pos++) {
			Object val = bindVariables[n];
			int sqltype = bindTypes[n];
			switch (sqltype) {
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGNVARCHAR:
			case Types.NVARCHAR:
			case Types.NCHAR:
				bindString(ps, pos, (String) val);
				break;
			case Types.NUMERIC:
			case Types.DECIMAL:
				bindBigDecimal(ps, pos, (BigDecimal) val);
				break;
			case Types.DATE:
			    if (val!=null)
			        if (val instanceof java.sql.Date)
			            bindSQLDate(ps, pos, (Date) val, calendar);
			        else
                        bindDate(ps, pos, (java.util.Date) val, calendar);
			    else
                    bindSQLDate(ps, pos, (Date) val, calendar);
				break;
			case Types.FLOAT:
			case Types.DOUBLE:
				bindDouble(ps, pos, (Double) val);
				break;
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
				bindInteger(ps, pos, (Integer) val);
				break;
			case Types.BIGINT:
				bindLong(ps, pos, (Long) val);
				break;
			case Types.REAL:
				bindFloat(ps, pos, (Float) val);
				break;
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				bindByteArray(ps, pos, (byte[]) val);
				break;
			case Types.BIT:
			case Types.BOOLEAN:
				bindBoolean(ps, pos, (Boolean) val);
				break;
			case Types.TIME:
			case Types.TIME_WITH_TIMEZONE: // TODO: mapping correct?
				bindSQLTime(ps, pos, (java.sql.Time) val);
				break;
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:  // TODO: mapping correct?
				if (val==null || val instanceof java.sql.Timestamp)
					bindSQLTimestamp(ps, pos, (java.sql.Timestamp) val, calendar);
				else
					bindSQLTimestamp(ps, pos, new java.sql.Timestamp(((java.util.Date)val).getTime()), calendar);
				break;
			default:
				throw new IllegalArgumentException(
						"unmatched value bindTypes[" + n + "], value=" + bindTypes[n]);
			}
		}
	}


}
