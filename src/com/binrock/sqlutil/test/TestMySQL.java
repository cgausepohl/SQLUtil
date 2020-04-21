package com.binrock.sqlutil.test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import com.binrock.sqlutil.SQLUtilInterface;

public class TestMySQL implements TestDB {

    private static final String[] sqls = {
            "select $col from sqlutil_data where $col is null limit 1", //0
            "select $col from sqlutil_data where $col is not null limit 1", //1
            "select $col from sqlutil_data where $col is null limit ?", //2
            "select $col from sqlutil_data where $col is not null limit ?", //3
            "select $col from sqlutil_data where $col is null limit ?", //4
            "select $col from sqlutil_data where $col is not null limit ?", //5
            "select $col from sqlutil_data where $col is null and (? is null or ?=v_serial) limit ?", //6
            "select $col from sqlutil_data where $col is not null and (? is null or ?=v_serial) limit ?", //7
            "select $col from sqlutil_data where $col is null limit 100", //8
            "select $col from sqlutil_data where $col is null and v_serial>? limit 100", //9
            "select $col from sqlutil_data where $col is null and v_serial>? limit 100", //10
            "select $col from sqlutil_data where $col is null and (? is null or $col =?) and v_serial>? limit 100"//11
    };

    @Override
    public void initSchema(SQLUtilInterface sql, char mode) throws SQLException {
        // type getter
        if ('G' == mode || '*' == mode) {
            sql.executeDDL("drop table if exists sqlutil_data");
            sql.executeDDL(
                    "create table sqlutil_data (v_integer integer,v_smallint smallint,v_int int\n"
                            + ",v_serial serial,v_numeric18 numeric(18),v_numeric38 numeric(38)\n"
                            + ",v_numeric18_5 numeric(18,5),v_float7 float(7),v_float8 float8,v_real real\n"
                            + ",v_date date,v_time time,v_timestamp timestamp,v_timestamptz timestamp\n"
                            + ",v_boolean boolean,v_bytea mediumblob,v_varchar100 varchar(100),v_char10 char(10)\n"
                            + ",v_text mediumtext)");
        }
        // performance
        if ('P' == mode || '*' == mode) {
            sql.executeDDL("drop table if exists sqlutil_addr");
            sql.executeDDL("drop table if exists sqlutil_cust");
            sql.executeDDL(
                    "create table sqlutil_cust(cust_id int primary key, d date, email varchar(100))");
            sql.executeDDL(
                    "create table sqlutil_addr(addr_id serial primary key, cust_id int references sqlutil_cust, addr varchar(200))");
        }
        sql.commit();
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

        l = sql.getStringVarArgs(mySqls[++n], new Integer(1));
        Test.test(l == null, typ + n);
        l = sql.getStringVarArgs(mySqls[++n], new Integer(1));
        Test.test(l != null, typ + n);

        Object[] bindVars = { new Integer(1) };
        l = sql.getString(mySqls[++n], bindVars);
        Test.test(l == null, typ + n);
        l = sql.getString(mySqls[++n], bindVars);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null, new Integer(1) };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        l = sql.getString(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { new Integer(1), new Integer(2), new Integer(1) };
        l = sql.getString(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getStrings
        String[] ll = sql.getStrings(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getStringsVarArgs(mySqls[++n], new Integer(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { new Integer(0) };
        ll = sql.getStrings(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars4 = { null, "bla", new Integer(0) };
        int[] sqlTypes4 = { Types.INTEGER, Types.VARCHAR, Types.INTEGER };
        //select $col from sqlutil_data where $col is null and (? is null or $col =?) and v_serial>? limit 100
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

        l = sql.getLongVarArgs(mySqls[++n], new Integer(1));
        Test.test(l == null, typ + n);
        l = sql.getLongVarArgs(mySqls[++n], new Integer(1));
        Test.test(l != null, typ + n);

        Object[] bindVars = { new Integer(1) };
        l = sql.getLong(mySqls[++n], bindVars);
        Test.test(l == null, typ + n);
        l = sql.getLong(mySqls[++n], bindVars);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null, new Integer(1) };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        l = sql.getLong(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { new Integer(1), new Integer(2), new Integer(1) };
        l = sql.getLong(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getStrings
        Long[] ll = sql.getLongs(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getLongsVarArgs(mySqls[++n], new Integer(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { new Integer(0) };
        ll = sql.getLongs(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);

        Object[] bindVars4 = { null, null, new Integer(0) };
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

        l = sql.getBigDecimalVarArgs(mySqls[++n], new Integer(1));
        Test.test(l == null, typ + n);
        l = sql.getBigDecimalVarArgs(mySqls[++n], new Integer(1));
        Test.test(l != null, typ + n);

        Object[] bindVars = { new Integer(1) };
        l = sql.getBigDecimal(mySqls[++n], bindVars);
        Test.test(l == null, typ + n);
        l = sql.getBigDecimal(mySqls[++n], bindVars);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null, new Integer(1) };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        l = sql.getBigDecimal(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { new Integer(1), new Integer(2), new Integer(1) };
        l = sql.getBigDecimal(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getBigDecimals
        BigDecimal[] ll = sql.getBigDecimals(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getBigDecimalsVarArgs(mySqls[++n], new Integer(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { new Integer(0) };
        ll = sql.getBigDecimals(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars4 = { null, null, new Integer(0) };
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
        l = sql.getTimestampVarArgs(mySqls[++n], new Integer(1));
        Test.test(l == null, typ + n);
        l = sql.getTimestampVarArgs(mySqls[++n], new Integer(1));
        Test.test(l != null, typ + n);

        Object[] bindVars = { new Integer(1) };
        l = sql.getTimestamp(mySqls[++n], bindVars);
        Test.test(l == null, typ + n);
        l = sql.getTimestamp(mySqls[++n], bindVars);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null, new Integer(1) };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        l = sql.getTimestamp(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { new Integer(1), new Integer(2), new Integer(1) };
        l = sql.getTimestamp(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getBigDecimals
        Timestamp[] ll = sql.getTimestamps(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getTimestampsVarArgs(mySqls[++n], new Integer(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { new Integer(0) };
        ll = sql.getTimestamps(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars4 = { null, null, new Integer(0) };
        int[] sqlTypes4 = { Types.DATE, Types.DATE, Types.INTEGER };
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

        l = sql.getRawVarArgs(mySqls[++n], new Integer(1));
        Test.test(l == null, typ + n);
        l = sql.getRawVarArgs(mySqls[++n], new Integer(1));
        Test.test(l != null, typ + n);

        Object[] bindVars = { new Integer(1) };
        l = sql.getRaw(mySqls[++n], bindVars);
        Test.test(l == null, typ + n);
        l = sql.getRaw(mySqls[++n], bindVars);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null, new Integer(1) };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        l = sql.getRaw(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { new Integer(1), new Integer(2), new Integer(1) };
        l = sql.getRaw(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getRaws
        byte[][] ll = sql.getRaws(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getRawsVarArgs(mySqls[++n], new Integer(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { new Integer(0) };
        ll = sql.getRaws(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars4 = { null, null, new Integer(0) };
        int[] sqlTypes4 = { Types.VARBINARY, Types.VARBINARY, Types.INTEGER };
        ll = sql.getRaws(mySqls[++n], bindVars4, sqlTypes4);
        Test.test(ll != null && ll.length == 100, typ + n);
    }

}
