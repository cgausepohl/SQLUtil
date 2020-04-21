package com.binrock.sqlutil.examples;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;

import com.binrock.sqlutil.Row;
import com.binrock.sqlutil.SQLUtilFactory;
import com.binrock.sqlutil.SQLUtilInterface;

public class Examples {

    static long logPrevLog = 0;
    static DateFormat logDF = DateFormat.getDateTimeInstance();

    static void log(String s) {
        long now = System.currentTimeMillis();
        long msToPrevLog = logPrevLog == 0 ? -1 : now - logPrevLog;
        logPrevLog = now;
        System.out.printf("[" + logDF.format(new Date(now)) + "][%1$4sms] " + s + "\n",
                msToPrevLog);
    }

    static void getStringExample(SQLUtilInterface sql) throws SQLException {
        String s = sql.getString("select 'Hello'");
        log("getString:" + s);
    }

    static void createTableExample(SQLUtilInterface sql) throws SQLException {
        sql.executeDDL("drop table if exists SQLUTIL_EXAMPLE");
        sql.executeDDL("create table SQLUTIL_EXAMPLE(id int, d date, d_str varchar(200))");
    }

    static void insertExample(SQLUtilInterface sql) throws SQLException {
        // as a string, no prepared statements, sql-injections possible
        Long id = 1L;
        String d = "2020-12-31";
        sql.executeDML("insert into SQLUTIL_EXAMPLE (id,d) values (" + id + ",to_date('" + d
                + "','YYYY-MM-DD'))");
        // all bind variables are !=null, with varargs
        sql.executeDMLVarArgs("insert into SQLUTIL_EXAMPLE (id,d) values (?,?)", new Long(2),
                new java.util.Date());
        // all bind variables are !=null, with array
        Object[] values = { new Long(3), new java.util.Date() };
        sql.executeDML("insert into SQLUTIL_EXAMPLE (id,d) values (?,?)", values);
        // nullable values needs type-mapping
        int[] sqltypes = { Types.BIGINT, Types.DATE };
        Object[] valuesWithNull = { new Long(4), null };
        sql.executeDML("insert into SQLUTIL_EXAMPLE (id,d) values (?,?)", valuesWithNull, sqltypes);
    }

    static void updateExample(SQLUtilInterface sql) throws SQLException {
        sql.executeDML("update SQLUTIL_EXAMPLE set d_str=to_char(d,'yyyy-mm-dd month day')");
    }

    static void selectExample(SQLUtilInterface sql) throws SQLException {
        Row[] rows = sql.getRows("select id,d,d_str from SQLUTIL_EXAMPLE order by id");
        for (Row row : rows) {
            Long id = row.getLong(0);
            Timestamp d = row.getTimestamp(1);
            String dStr = row.getString(2);
            log("[id=" + id + "; d=" + d + "; d_str=" + dStr + "]");
        }
    }

    static void timingExample(SQLUtilInterface sql) throws SQLException {
        String sqlStmt = "vacuum full analyze SQLUTIL_EXAMPLE(id,d,d_str)";
        sql.executeDDL(sqlStmt);
        log(sqlStmt);
        log("  " + sql.getAudit().getAuditRecords().get(sqlStmt).get(0).toString());
    }

    static void chunkExample(SQLUtilInterface sql) throws SQLException {
        /**
         * assuming that SQLUTIL_EXAMPLE contains 4 rows. 4*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2=1048576
         */
        // generate data
        log("create 131072 rows");
        for (int i = 0; i < 15; i++)
            sql.executeDML("insert into SQLUTIL_EXAMPLE select * from SQLUTIL_EXAMPLE");
        log("created");

        // read data as chunks
        // each chunk has a maxSize of 50000 rows, that limits the max memory usage
        sql.getChunksPrepare("select id,d,d_str from SQLUTIL_EXAMPLE", 50000);
        int chunkNum = 0;
        Row[] rows = null;
        while ((rows = sql.getChunksGetNextRows()) != null) {
            log("done chunk #" + chunkNum + ", size=" + rows.length);
            chunkNum++;
        }
        sql.getChunksClose();
        log("all chunks done");
    }

    static void batchedInsertExample(SQLUtilInterface sql) throws SQLException {
        // create an array of 100.000 rows, insert them at once
        log("generate 100000 new rows inmemory");
        List<Object[]> batchValues = new ArrayList<>();
        DateFormat df = DateFormat.getInstance();
        for (int i = 0; i < 100000; i++) {
            Object[] row = new Object[3];
            row[0] = new Long(i);
            Date d = new Date(System.currentTimeMillis());
            row[1] = d;
            // sometimes null
            if (i % 4 != 0)
                row[2] = df.format(d) + " - " + row[0];
            batchValues.add(row);
        }
        int[] bindTypes = { Types.BIGINT, Types.DATE, Types.VARCHAR };
        log("inserting...");
        sql.executeDMLBatch("insert into SQLUTIL_EXAMPLE(id,d,d_str) values (?,?,?)", batchValues,
                bindTypes);
        log("inserted");
    }

    static void batchedUpdateExample(SQLUtilInterface sql) throws SQLException {
        // assume batchedInsertExample ran before, expect 100.000 rows, with id 0..99999
        log("generate 100000 changes in memory");
        List<Object[]> batchValues = new ArrayList<>();
        DateFormat df = DateFormat.getInstance();
        for (int i = 0; i < 100000; i++) {
            Object[] row = new Object[3];
            Date d = new Date(System.currentTimeMillis());
            row[0] = d;
            // sometimes null
            if (i % 3 != 0)
                row[1] = df.format(d) + " - " + row[0];
            row[2] = new Long(i);
            batchValues.add(row);
        }
        int[] bindTypes = { Types.DATE, Types.VARCHAR, Types.BIGINT };
        log("creating index on column id...");
        sql.executeDDL("create index IDX_SQLUTIL_EXAMPLE on SQLUTIL_EXAMPLE(id)");
        log("updating...");
        int[] affectedRows = sql.executeDMLBatch(
                "update SQLUTIL_EXAMPLE set d=?,d_str=? where id=?", batchValues, bindTypes);
        int updCnt = 0;
        for (int i : affectedRows)
            updCnt += i;
        log("updated rows " + updCnt);
    }

    public static void main(String[] args) throws SQLException {
        SQLUtilInterface sql = null;
        try {
            sql = SQLUtilFactory.createSQLUtil("jdbc:postgresql://localhost/sqlutil", "sqlutil",
                    "sqlutil");
            /* // if you already have a connection or need a special initialization:
             * Connection con = DriverManager.getConnection("jdbc:postgresql://localhost/sqlutil", "sqlutil", "sqlutil");
             * sql = SQLUtilFactory.createSQLUtil(con);
             */
            getStringExample(sql);
            createTableExample(sql);
            insertExample(sql);
            updateExample(sql);
            selectExample(sql);
            timingExample(sql);
            chunkExample(sql);
            batchedInsertExample(sql);
            batchedUpdateExample(sql);

            sql.getAudit().printSummary(System.out, 0, null);

            System.out.println("done");
        } finally {
            if (sql != null)
                sql.closeConnection();
        }
    }

}
