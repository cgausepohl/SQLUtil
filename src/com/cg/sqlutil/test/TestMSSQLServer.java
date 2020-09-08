/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/SQLUtil
 */
package com.cg.sqlutil.test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import com.cg.sqlutil.SQLUtilInterface;

public class TestMSSQLServer implements TestDB {

    private static final String[] sqls = { "select top 1 $col from sqlutil_data where $col is null", //0
            "select top 1 $col from sqlutil_data where $col is not null", //1
            "select top 1 $col from sqlutil_data where $col is null and (? is null or ?=v_serial)", //2
            "select top 1 $col from sqlutil_data where $col is not null and (? is null or ?=v_serial)", //3
            "select top 100 $col from sqlutil_data where $col is null", //4
            "select top 100 $col from sqlutil_data where $col is null and v_serial>?", //5
            "select top 100 $col from sqlutil_data where $col is null and v_serial>?", //6
            "select top 100 $col from sqlutil_data where $col is null and (? is null or $col =?) and v_serial>?"//7
    };

    @Override
    public void initSchema(SQLUtilInterface sql, char mode) throws SQLException {
        // type getter
        if ('G' == mode || '*' == mode) {
            sql.executeDDL("drop table if exists sqlutil_data");
            sql.executeDDL(
                    "create table sqlutil_data (v_integer integer,v_smallint smallint,v_int int,\n"
                            + "v_serial int identity(1,1),v_numeric18 numeric(18),v_numeric38 numeric(38)\n"
                            + ",v_numeric18_5 numeric(18,5),v_float7 float(7),v_float8 float(8),v_real real\n"
                            + ",v_date date,v_time time,v_timestamp datetime2,v_timestamptz datetimeoffset, v_timestamp_auto timestamp\n"
                            + ",v_boolean bit,v_bytea varbinary(1000),v_varchar100 varchar(100),v_char10 char(10)\n"
                            + ",v_text text)");
        }
        // for performance
        if ('P' == mode || '*' == mode) {
            sql.executeDDL("drop table if exists sqlutil_addr");
            sql.executeDDL("drop table if exists sqlutil_cust");
            sql.executeDDL(
                    "create table sqlutil_cust(cust_id int primary key, d date, email varchar(100))");
            sql.executeDDL(
                    "create table sqlutil_addr(addr_id int primary key identity(1,1), cust_id int references sqlutil_cust, addr varchar(200))");
            sql.commit();
        }
    }

    @Override
    public void testRollback(SQLUtilInterface sql) throws SQLException {
        sql.rollback();
    }

    @Override
    public void testString(SQLUtilInterface sql) throws SQLException {
        String col = "v_varchar100";
        String typ = "get String test ";
        String[] mySqls = sqls.clone();
        for (int i = 0; i < mySqls.length; i++)
            mySqls[i] = mySqls[i].replace("$col", col);

        int n = 0;
        // getString
        String l = sql.getString(mySqls[n]);
        Test.test(l == null, typ + n);
        l = sql.getString(mySqls[++n]);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER };
        l = sql.getString(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { Integer.valueOf(1), Integer.valueOf(2) };
        l = sql.getString(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getStrings
        String[] ll = sql.getStrings(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getStringsVarArgs(mySqls[++n], Integer.valueOf(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { Integer.valueOf(0) };
        ll = sql.getStrings(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars4 = { null, null, Integer.valueOf(0) };
        int[] sqlTypes4 = { Types.INTEGER, Types.VARCHAR, Types.INTEGER };
        ll = sql.getStrings(mySqls[++n], bindVars4, sqlTypes4);
        Test.test(ll != null && ll.length == 100, typ + n);
    }

    @Override
    public void testLong(SQLUtilInterface sql) throws SQLException {
        String col = "v_numeric18";
        String typ = "get Long test ";
        String[] mySqls = sqls.clone();
        for (int i = 0; i < mySqls.length; i++)
            mySqls[i] = mySqls[i].replace("$col", col);

        int n = 0;
        // getLong
        Long l = sql.getLong(mySqls[n]);
        Test.test(l == null, typ + n);
        l = sql.getLong(mySqls[++n]);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER };
        l = sql.getLong(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { Integer.valueOf(1), Integer.valueOf(2) };
        l = sql.getLong(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getLongs
        Long[] ll = sql.getLongs(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getLongsVarArgs(mySqls[++n], Integer.valueOf(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { Integer.valueOf(0) };
        ll = sql.getLongs(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars4 = { null, null, Integer.valueOf(0) };
        int[] sqlTypes4 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        ll = sql.getLongs(mySqls[++n], bindVars4, sqlTypes4);
        Test.test(ll != null && ll.length == 100, typ + n);
    }

    @Override
    public void testBigDecimal(SQLUtilInterface sql) throws SQLException {
        String col = "v_numeric38";
        String typ = "get BigDecimal test ";
        String[] mySqls = sqls.clone();
        for (int i = 0; i < mySqls.length; i++)
            mySqls[i] = mySqls[i].replace("$col", col);

        int n = 0;
        // getBigDecimal
        BigDecimal l = sql.getBigDecimal(mySqls[n]);
        Test.test(l == null, typ + n);
        l = sql.getBigDecimal(mySqls[++n]);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER };
        l = sql.getBigDecimal(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { Integer.valueOf(1), Integer.valueOf(2) };
        l = sql.getBigDecimal(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getBigDecimals
        BigDecimal[] ll = sql.getBigDecimals(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getBigDecimalsVarArgs(mySqls[++n], Integer.valueOf(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { Integer.valueOf(0) };
        ll = sql.getBigDecimals(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars4 = { null, null, Integer.valueOf(0) };
        int[] sqlTypes4 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        ll = sql.getBigDecimals(mySqls[++n], bindVars4, sqlTypes4);
        Test.test(ll != null && ll.length == 100, typ + n);
    }

    @Override
    public void testTimestamp(SQLUtilInterface sql) throws SQLException {
        String col = "v_timestamp";
        String typ = "get Timestamp test ";
        String[] mySqls = sqls.clone();
        for (int i = 0; i < mySqls.length; i++)
            mySqls[i] = mySqls[i].replace("$col", col);

        int n = 0;
        // getTimestamp
        Timestamp l = sql.getTimestamp(mySqls[n]);
        Test.test(l == null, typ + n);
        l = sql.getTimestamp(mySqls[++n]);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER };
        l = sql.getTimestamp(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { Integer.valueOf(1), Integer.valueOf(2) };
        l = sql.getTimestamp(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getTimestamps
        Timestamp[] ll = sql.getTimestamps(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getTimestampsVarArgs(mySqls[++n], Integer.valueOf(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { Integer.valueOf(0) };
        ll = sql.getTimestamps(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars4 = { null, null, Integer.valueOf(0) };
        int[] sqlTypes4 = { Types.INTEGER, Types.TIMESTAMP, Types.INTEGER };
        ll = sql.getTimestamps(mySqls[++n], bindVars4, sqlTypes4);
        Test.test(ll != null && ll.length == 100, typ + n);
    }

    @Override
    public void testRaw(SQLUtilInterface sql) throws SQLException {
        String col = "v_bytea";
        String typ = "get byte[] test ";
        String[] mySqls = sqls.clone();
        for (int i = 0; i < mySqls.length; i++)
            mySqls[i] = mySqls[i].replace("$col", col);

        int n = 0;
        // getRaw
        byte[] l = sql.getRaw(mySqls[n]);
        Test.test(l == null, typ + n);
        l = sql.getRaw(mySqls[++n]);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER };
        l = sql.getRaw(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { Integer.valueOf(1), Integer.valueOf(2) };
        l = sql.getRaw(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getRaws
        byte[][] ll = sql.getRaws(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getRawsVarArgs(mySqls[++n], Integer.valueOf(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { Integer.valueOf(0) };
        ll = sql.getRaws(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars4 = { null, null, Integer.valueOf(0) };
        int[] sqlTypes4 = { Types.INTEGER, Types.VARBINARY, Types.INTEGER };
        ll = sql.getRaws(mySqls[++n], bindVars4, sqlTypes4);
        Test.test(ll != null && ll.length == 100, typ + n);
    }

}
