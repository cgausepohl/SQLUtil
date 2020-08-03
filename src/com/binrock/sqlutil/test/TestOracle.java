package com.binrock.sqlutil.test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import com.binrock.sqlutil.SQLUtilInterface;

public class TestOracle implements TestDB {

    private static final String[] sqls = {
            "select $col from sqlutil_data where $col is null and rownum<=1", //0
            "select $col from sqlutil_data where $col is not null and rownum<=1", //1
            "select $col from sqlutil_data where $col is null  and rownum<=?", //2
            "select $col from sqlutil_data where $col is not null  and rownum<=?", //3
            "select $col from sqlutil_data where $col is null  and rownum<=?", //4
            "select $col from sqlutil_data where $col is not null  and rownum<=?", //5
            "select $col from sqlutil_data where $col is null and (? is null or ?=v_serial) and rownum<=?", //6
            "select $col from sqlutil_data where $col is not null and (? is null or ?=v_serial) and rownum<=?", //7
            "select $col from sqlutil_data where $col is null and rownum<=100", //8
            "select $col from sqlutil_data where $col is null and v_serial>? and rownum<=100", //9
            "select $col from sqlutil_data where $col is null and v_serial>? and rownum<=100", //10
            "select $col from sqlutil_data where $col is null and (? is null or $col=?) and v_serial>? and rownum<=100"//11
    };

    @Override
    public void initSchema(SQLUtilInterface sql, char mode) throws SQLException {
        // for getter
        if ('G' == mode || '*' == mode) {
            sql.executeDDLSilent("drop table sqlutil_data");
            sql.executeDDLSilent("drop sequence seq_sqlutildata_vserial");
            sql.executeDDL("create sequence seq_sqlutildata_vserial");
            sql.executeDDL(
                    "create table sqlutil_data (v_integer integer,v_smallint smallint,v_int int\n"
                            + ",v_serial int,v_numeric18 number(18),v_numeric38 number(38)\n"
                            + ",v_numeric18_5 number(18,5),v_float7 BINARY_DOUBLE,v_float8 BINARY_DOUBLE,v_real real\n"
                            + ",v_date date,v_time date,v_timestamp timestamp,v_timestamptz timestamp with time zone\n"
                            + ",v_boolean char(1),v_bytea raw(1000),v_varchar100 varchar2(100),v_char10 char(10)\n"
                            + ",v_text clob)");
            sql.executeDDL(
                    "begin execute immediate 'CREATE OR REPLACE TRIGGER trg_sqlutildata_vserial "
                            + "before insert on sqlutil_data for each row "
                            + "begin :new.v_serial := seq_sqlutildata_vserial.nextval; end;'; end;");
        }
        if ('P' == mode || '*' == mode) {
            // for performance
            sql.executeDDLSilent("DROP TABLE sqlutil_cust CASCADE constraints");
            sql.executeDDLSilent("DROP TABLE sqlutil_addr CASCADE constraints");
            sql.executeDDLSilent("DROP SEQUENCE seq_sqlutil_addr");
            sql.executeDDL(
                    "create table sqlutil_cust(cust_id int primary key, d date, email varchar2(100))");
            sql.executeDDL(
                    "create table sqlutil_addr(addr_id int primary key, cust_id int references sqlutil_cust, addr varchar(200))");
            sql.executeDDL("CREATE SEQUENCE seq_sqlutil_addr");
            sql.executeDDL(
                    "begin execute immediate 'CREATE OR REPLACE TRIGGER trg_sqlutiladdr_pk BEFORE INSERT ON sqlutil_addr FOR EACH ROW "
                            + "begin :new.addr_id := seq_sqlutil_addr.nextval; end;'; end;");
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

        l = sql.getStringVarArgs(mySqls[++n], Integer.valueOf(1));
        Test.test(l == null, typ + n);
        l = sql.getStringVarArgs(mySqls[++n], Integer.valueOf(1));
        Test.test(l != null, typ + n);

        Object[] bindVars = { Integer.valueOf(1) };
        l = sql.getString(mySqls[++n], bindVars);
        Test.test(l == null, typ + n);
        l = sql.getString(mySqls[++n], bindVars);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null, Integer.valueOf(1) };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        l = sql.getString(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(1) };
        l = sql.getString(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getLongs
        String[] ll = sql.getStrings(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getStringsVarArgs(mySqls[++n], Integer.valueOf(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { Integer.valueOf(0) };
        ll = sql.getStrings(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);

        Object[] bindVars4 = { null, null, Integer.valueOf(0) };
        int[] sqlTypes4 = { Types.VARCHAR, Types.VARCHAR, Types.INTEGER };
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

        l = sql.getLongVarArgs(mySqls[++n], Integer.valueOf(1));
        Test.test(l == null, typ + n);
        l = sql.getLongVarArgs(mySqls[++n], Integer.valueOf(1));
        Test.test(l != null, typ + n);

        Object[] bindVars = { Integer.valueOf(1) };
        l = sql.getLong(mySqls[++n], bindVars);
        Test.test(l == null, typ + n);
        l = sql.getLong(mySqls[++n], bindVars);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null, Integer.valueOf(1) };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        l = sql.getLong(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(1) };
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
        int[] sqlTypes4 = { Types.BIGINT, Types.BIGINT, Types.INTEGER };
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

        l = sql.getBigDecimalVarArgs(mySqls[++n], Integer.valueOf(1));
        Test.test(l == null, typ + n);
        l = sql.getBigDecimalVarArgs(mySqls[++n], Integer.valueOf(1));
        Test.test(l != null, typ + n);

        Object[] bindVars = { Integer.valueOf(1) };
        l = sql.getBigDecimal(mySqls[++n], bindVars);
        Test.test(l == null, typ + n);
        l = sql.getBigDecimal(mySqls[++n], bindVars);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null, Integer.valueOf(1) };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        l = sql.getBigDecimal(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(1) };
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
        int[] sqlTypes4 = { Types.BIGINT, Types.BIGINT, Types.INTEGER };
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
        l = sql.getTimestampVarArgs(mySqls[++n], Integer.valueOf(1));
        Test.test(l == null, typ + n);
        l = sql.getTimestampVarArgs(mySqls[++n], Integer.valueOf(1));
        Test.test(l != null, typ + n);

        Object[] bindVars = { Integer.valueOf(1) };
        l = sql.getTimestamp(mySqls[++n], bindVars);
        Test.test(l == null, typ + n);
        l = sql.getTimestamp(mySqls[++n], bindVars);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null, Integer.valueOf(1) };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        l = sql.getTimestamp(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(1) };
        l = sql.getTimestamp(mySqls[++n], bindVars2b, sqlTypes2);
        Test.test(l != null, typ + n);

        // getBigDecimals
        Timestamp[] ll = sql.getTimestamps(mySqls[++n]);
        Test.test(ll != null && ll.length == 100, typ + n);
        ll = sql.getTimestampsVarArgs(mySqls[++n], Integer.valueOf(0));
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars3 = { Integer.valueOf(0) };
        ll = sql.getTimestamps(mySqls[++n], bindVars3);
        Test.test(ll != null && ll.length == 100, typ + n);
        Object[] bindVars4 = { null, null, Integer.valueOf(0) };
        int[] sqlTypes4 = { Types.TIMESTAMP, Types.TIMESTAMP, Types.INTEGER };
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

        l = sql.getRawVarArgs(mySqls[++n], Integer.valueOf(1));
        Test.test(l == null, typ + n);
        l = sql.getRawVarArgs(mySqls[++n], Integer.valueOf(1));
        Test.test(l != null, typ + n);

        Object[] bindVars = { Integer.valueOf(1) };
        l = sql.getRaw(mySqls[++n], bindVars);
        Test.test(l == null, typ + n);
        l = sql.getRaw(mySqls[++n], bindVars);
        Test.test(l != null, typ + n);

        Object[] bindVars2a = { null, null, Integer.valueOf(1) };
        int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
        l = sql.getRaw(mySqls[++n], bindVars2a, sqlTypes2);
        Test.test(l == null, typ + n);
        Object[] bindVars2b = { Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(1) };
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
        int[] sqlTypes4 = { Types.VARBINARY, Types.VARBINARY, Types.INTEGER };
        ll = sql.getRaws(mySqls[++n], bindVars4, sqlTypes4);
        Test.test(ll != null && ll.length == 100, typ + n);
    }

}
