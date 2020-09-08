/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/SQLUtil
 */
package com.cg.sqlutil.impl;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Hashtable;
import java.util.List;

import com.cg.sqlutil.Row;

public final class ConversionHelper {

    public static Integer toInteger(Object o) {
        try {
            if (o == null)
                return null;
            return toLong(o).intValue();
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException(
                    "cannot convert class " + o.getClass().getName() + " to Integer");
        }
    }

    public static Long toLong(Object o) {
        if (o == null)
            return null;
        if (o instanceof Long)
            return (Long) o;
        if (o instanceof BigDecimal)
            return ((BigDecimal) o).longValue();
        if (o instanceof Integer)
            return Long.valueOf((Integer) o);
        if (o instanceof Short)
            return Long.valueOf((Short) o);
        if (o instanceof Byte)
            return Long.valueOf((Byte) o);
        if (o instanceof String)
            return Long.valueOf((String) o);
        else
            throw new IllegalArgumentException(
                    "cannot convert class " + o.getClass().getName() + " to Long");
    }

    public static BigDecimal toBigDecimal(Object o) {
        if (o == null)
            return null;
        if (o instanceof BigDecimal)
            return (BigDecimal) o;
        if (o instanceof Long)
            return new BigDecimal((Long) o);
        if (o instanceof Double)
            return new BigDecimal((Double) o);
        if (o instanceof Float)
            return new BigDecimal((Float) o);
        if (o instanceof Integer)
            return new BigDecimal((Integer) o);
        if (o instanceof Short)
            return new BigDecimal((Short) o);
        if (o instanceof Byte)
            return new BigDecimal((Byte) o);
        if (o instanceof String)
            return new BigDecimal((String) o);
        else
            throw new IllegalArgumentException(
                    "cannot convert class " + o.getClass().getName() + " to BigDecimal");
    }

    public static Double[] toDoubles(Row[] rows) {
        Double[] arr = new Double[rows == null ? 0 : rows.length];
        if (rows == null)
            return arr;
        int i = 0;
        for (Row row : rows)
            arr[i++] = row.getDouble(0);
        return arr;
    }

    public static Double toDouble(Object o) {
        Double d;
        if (o == null)
            d = null;
        else if (o instanceof Double)
            d = (Double) o;
        else if (o instanceof Float)
            d = Double.valueOf((Float) o);
        else if (o instanceof BigDecimal)
            d = ((BigDecimal) o).doubleValue();
        else if (o instanceof Long)
            d = Double.valueOf((Long) o);
        else if (o instanceof Byte)
            d = Double.valueOf((Byte) o);
        else if (o instanceof Integer)
            d = Double.valueOf((Integer) o);
        else if (o instanceof Short)
            d = Double.valueOf((Short) o);
        else if (o instanceof String)
            d = Double.valueOf((String) o);
        else
            throw new IllegalArgumentException(
                    "cannot convert class " + o.getClass().getName() + " to Double");
        return d;
    }

    public static BigDecimal[] toBigDecimals(Row[] rows) {
        BigDecimal[] arr = new BigDecimal[rows == null ? 0 : rows.length];
        if (rows == null)
            return arr;
        int i = 0;
        for (Row row : rows)
            arr[i++] = row.getBigDecimal(0);
        return arr;
    }

    public static byte[][] toRaws(Row[] rows) {
        byte[][] arr = new byte[rows == null ? 0 : rows.length][];
        if (rows == null)
            return arr;
        int i = 0;
        for (Row row : rows) {
            if (row != null) {
                byte[] test = { 0, 1, 2 };
                arr[i++] = test;
            }
        }
        return arr;
    }

    public static Long[] toLongs(Row[] rows) {
        Long[] arr = new Long[rows == null ? 0 : rows.length];
        if (rows == null)
            return arr;
        int i = 0;
        for (Row row : rows)
            arr[i++] = row.getLong(0);
        return arr;
    }

    public static java.sql.Timestamp[] toTimestamps(Row[] rows) {
        Timestamp[] arr = new Timestamp[rows == null ? 0 : rows.length];
        if (rows == null)
            return arr;
        int i = 0;
        for (Row row : rows)
            arr[i++] = row.getTimestamp(0);
        return arr;
    }

    public static String toStr(Object o) {
        return o == null ? null : o.toString();
    }

    public static String[] toStrings(Row[] rows) {
        String[] arr = new String[rows == null ? 0 : rows.length];
        if (rows == null)
            return arr;
        int i = 0;
        for (Row row : rows)
            arr[i++] = row.getString(0);
        return arr;
    }

    public static java.sql.Timestamp toTimestamp(Object o) {
        Timestamp ts;
        if (o == null)
            ts = null;
        else if (o instanceof java.sql.Timestamp)
            ts = (java.sql.Timestamp) o;
        else if (o instanceof java.util.Date) // handles also hava.sql.Date
            ts = new Timestamp(((java.util.Date) o).getTime());
        else
            throw new IllegalArgumentException(
                    "cannot convert class " + o.getClass().getName() + " to java.sql.Timestamp");
        return ts;
    }

    public static Boolean toBoolean(Object o) {
        Boolean b;
        if (o == null)
            b = null;
        else if (o instanceof Boolean)
            b = (Boolean) o;
        else
            throw new IllegalArgumentException(
                    "cannot convert class " + o.getClass().getName() + " to Boolean");
        return b;
    }

    public static Hashtable<Object, Object> toHashtable(List<Object[]> rows) {
        Hashtable<Object, Object> h = new Hashtable<>();
        for (Object[] row : rows)
            h.put(row[0], row[1]);
        return h;
    }

    // create more predefined types if necessary
    public static Hashtable<String, String> toHashtableStringString(List<Object[]> rows) {
        Hashtable<String, String> h = new Hashtable<>();
        for (Object[] row : rows)
            h.put(toStr(row[0]), toStr(row[1]));
        return h;
    }

    public static String toRaw(Object object) {
    	// may be byte[]
        throw new IllegalStateException("not implemented");
    }

}
