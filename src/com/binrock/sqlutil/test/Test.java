package com.binrock.sqlutil.test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.binrock.sqlutil.AuditInterface.StoreBindVariables;
import com.binrock.sqlutil.AuditInterface.UseBatchInserts;
import com.binrock.sqlutil.AuditInterface.UseCurrentThread;
import com.binrock.sqlutil.Row;
import com.binrock.sqlutil.SQLUtilFactory;
import com.binrock.sqlutil.SQLUtilInterface;

public class Test {

    static final Random rand = new Random();
    static final long ROWSCREATE = 200/*200 is minimum, otherwise tests will fail*/,
            EXECSELECTS = 2;
    static final String SQL_CUST_INS = "insert into sqlutil_cust(cust_id,d,email)values(?,?,?)";
    static final String SQL_ADDR_INS = "insert into sqlutil_addr(cust_id,addr)values(?,?)";

    // expecting a postgres-db with user sqlutil and password SQLutil$1
    // tables and data will be set up during this test
    public static void main(String[] args) throws SQLException, InterruptedException {
        //new Test().testAll("jdbc:postgresql://localhost/sqlutil", "sqlutil", "sqlutil");
        //new Test().testAll("jdbc:sqlserver://192.168.188.79:1433;databaseName=TEST", "sqlutil", "sqlutil");
        //new Test().testAll("jdbc:oracle:thin:@192.168.188.79:1521/xe", "sqlutil", "sqlutil");
        // TODO reduce options
        //new Test().testAll("jdbc:mysql://localhost:3306/sqlutil?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC", "sqlutil", "sqlutil");
    }

    private void testAll(String jdbc, String user, String password)
            throws SQLException, InterruptedException {
        System.out.println("starting");
        SQLUtilInterface sql = null;
        long programStarted = System.currentTimeMillis();
        try {
            sql = SQLUtilFactory.createSQLUtil(jdbc, user, password,
                    SQLUtilFactory.RWMODE_READWRITE, SQLUtilFactory.AUTOCOMMIT_ON,
                    Connection.TRANSACTION_READ_COMMITTED);
            System.out.println("DBProduct " + sql.getDBProduct() + " " + jdbc);

            TestDB testDB;
            switch (sql.getDBProduct()) {
            case POSTGRESQL:
                testDB = new TestPostgreSQL();
                break;
            case MSSQLSERVER:
                testDB = new TestMSSQLServer();
                break;
            case ORACLE:
                testDB = new TestOracle();
                break;
            case MYSQL:
                testDB = new TestMySQL();
                break;
            default:
                testDB = null;
            }

            testDB.initSchema(sql, 'G');
            System.out.println("fill table, " + ROWSCREATE + " rows...");
            createData(sql);
            // just to have it called once
            testDB.testRollback(sql);

            // get counts with different types
            System.out.println("get counts with different types, " + EXECSELECTS + " times...");
            testGetCounts(sql);

            // ALL TESTS
            // test: types
            Test.log("test getString* methods");
            testDB.testString(sql);
            Test.log("done");

            Test.log("test getLong* methods");
            testDB.testLong(sql);
            Test.log("done");

            Test.log("test getBigDecimal* methods");
            testDB.testBigDecimal(sql);
            Test.log("done");

            Test.log("test getTimestamp* methods");
            testDB.testTimestamp(sql);
            Test.log("done");

            Test.log("test getRaw* methods");
            testDB.testRaw(sql);
            Test.log("done");

            // test: stream
            Test.log("test stream methods");
            testStream(sql);
            Test.log("done");

            // test: jdbc/batched dml/performance test
            Test.log("test jdbc/batched dml/performance");
            /* Program to compare different implementations for different kind of database tasks.
               Two tables are created: sqlutil_cust("+Test.CUST_ROWS+" rows), sqlutil_addr(0..3 rows per customer).
               1) (drop/create), populate via loop(sqlutil): for all cust; create cust and create 0..2 addr rows.
               2) (drop/create), populate via loop(jdbc direct): for all cust; create cust and create 0..2 addr rows.
               3) (drop/create), populate via batch: one batch for cust, one batch for addr
            */
            testPerformance(sql, testDB);
            Test.log("done");

            // test: audit
            Test.log("Audit storing...");
            sql.getAudit().setConnectionData(jdbc, user, password);
            sql.getAudit().store(UseCurrentThread.YES, Test.class.getSimpleName(),
                    StoreBindVariables.YES, UseBatchInserts.YES);
            Test.log("done");
        } finally {
            if (sql != null)
                sql.closeConnection();
            //sql.getAudit().printSummary(System.out,  0, null);
        }

        System.out.println(sql.getDBProduct() + " done, exec time(ms)="
                + (System.currentTimeMillis() - programStarted));
    }

    private static void testStream(SQLUtilInterface sql) throws SQLException {
        System.out.println("test getChunks* methods");
        sql.getChunksPrepare("select v_integer from sqlutil_data", (int) (ROWSCREATE / 20));
        Row[] rows = null;
        long sum = 0, rowcount = 0;
        while (true) {
            rows = sql.getChunksGetNextRows();
            if (rows == null)
                break;
            rowcount += rows.length;
            for (Row row : rows)
                if (row != null && row.getLong(0) != null)
                    sum += row.getLong(0);
        }
        long cntAll = sql.getLong("select sum(v_integer) from sqlutil_data");
        test((cntAll - sum) == 0, "batch mode, getting sum(v_integer)");
        test((rowcount - ROWSCREATE) == 0, "batch mode, reading rows");
    }

    /*
    	public static void printPublicMembers() {
    		Class<Types> c = Types.class;
    		for (Field f : c.getDeclaredFields()) {
    			int mod = f.getModifiers();
    			if (Modifier.isStatic(mod) && Modifier.isPublic(mod) && Modifier.isFinal(mod)) {
    				try {
    					System.out.printf("%s = %d%n", f.getName(), f.get(null));
    				} catch (IllegalAccessException e) {
    					e.printStackTrace();
    				}
    			}
    		}
    	}
    */
    static void test(boolean b, String msg) {
        if (!b)
            throw new RuntimeException("Testcase failed; " + msg);
    }

    // create byte array, filled with 0xDEAD, length between 0 and 1000.
    static byte[] bytea() {
        int len = rand.nextInt(1000);
        byte[] a = new byte[len];
        for (int i = 0; i < len; i++) {
            switch (i % 4) {
            case 0:
                a[i] = 0xD;
                break;
            case 1:
                a[i] = 0xE;
                break;
            case 2:
                a[i] = 0xA;
                break;
            case 3:
                a[i] = 0xD;
                break;
            }
        }
        return a;
    }

    // generate a string, based on param i. 123 will return onetwothree, param len=max length of string.
    static String weirdString(int i, int len) {
        StringBuffer sb = new StringBuffer(100);
        if (i == 0)
            return "zero";
        String[] nums = { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight",
                "nine" };
        while (i != 0) {
            int mod = i % 10;
            sb.append(nums[mod]);
            i = i / 10;
        }
        return len > sb.length() ? sb.toString() : sb.toString().substring(0, len);
    }

    private static void createData(SQLUtilInterface sql) throws SQLException {
        Object[] allNulls = new Object[18];
        int[] allTypes = { Types.INTEGER, Types.SMALLINT, Types.INTEGER, Types.NUMERIC,
                Types.DECIMAL, Types.DECIMAL, Types.DOUBLE, Types.DOUBLE, Types.FLOAT, Types.DATE,
                Types.TIME, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE, Types.BOOLEAN,
                Types.VARBINARY, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR };
        for (int i = 1; i <= Test.ROWSCREATE; i++) {
            String insertStmt = "insert into sqlutil_data("
                    + "v_integer, v_smallint, v_int, v_numeric18, v_numeric38,"
                    + "v_numeric18_5, v_float7,v_float8, v_real,"
                    + "v_date, v_time, v_timestamp, v_timestamptz," + "v_boolean, v_bytea,"
                    + "v_varchar100, v_char10, v_text"
                    + ") values(/*ints*/?,?,?,?,?,  /*floats*/?,?,?,?,  /*date/times*/?,?,?,?,  /*bool,bytea*/?,?, /*strings*/?,?,?)";
            if (i % 2 == 0) {
                sql.executeDMLVarArgs(insertStmt, new Integer(i), new Integer(i), new Integer(i),
                        new BigDecimal(i), new BigDecimal(i), new Float(1), new Double(2),
                        new BigDecimal(4), new Float(4), new Date(),
                        new java.sql.Time(new Date().getTime()),
                        new java.sql.Timestamp(new Date().getTime()),
                        new java.sql.Timestamp(new Date().getTime()), Boolean.TRUE, Test.bytea(),
                        Test.weirdString(i, 30), Test.weirdString(i, 10),
                        Test.weirdString(i, 5000));
            } else {
                sql.executeDML(insertStmt, allNulls, allTypes);
            }
        }
        sql.commit();
    }

    private static void testGetCounts(SQLUtilInterface sql) throws SQLException {
        for (int i = 0; i < Test.EXECSELECTS; i++) {
            long l = sql.getLong("select count(*) from sqlutil_data");
            Test.test(l == Test.ROWSCREATE, "init test 1");
            BigDecimal bd = sql.getBigDecimal("select count(*) from sqlutil_data");
            Test.test(new BigDecimal(Test.ROWSCREATE).equals(bd), "init test 2");
            //pg String str = sql.getString("select cast(count(*)as text) from sqlutil_data");
            String str = sql.getString("select count(*) from sqlutil_data");
            Test.test(new Long(Test.ROWSCREATE).toString().equals(str), "init test 3");
        }
        Row[] fullData = sql
                .getRows("select v_integer,v_smallint,v_int,v_serial,v_numeric18,v_numeric38,"// 1
                        + "v_numeric18_5,v_float7,v_float8,v_real,"// 7
                        + "v_date,v_time,v_timestamp,v_timestamptz,"// 11
                        + "v_boolean,"// 15
                        + "v_varchar100,v_char10,v_text from sqlutil_data order by 1");// 16
        test(fullData.length == Test.ROWSCREATE, "test5");
    }

    static long logPrevLog = 0;
    static DateFormat logDF = DateFormat.getDateTimeInstance();

    static class RandomAddress {
        static final String CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";
        int custId;
        String addr;

        RandomAddress(int custId) {
            this.custId = custId;
            StringBuilder sb = new StringBuilder(50);
            int l = CHARS.length();
            for (int i = 0; i < 50; i++)
                sb.append(CHARS.charAt(rand.nextInt(l)));
            addr = sb.toString();
        }
    }

    static class RandomCustomer {
        int id;
        Date d;
        String email;
        List<RandomAddress> addresses = new ArrayList<>();

        RandomCustomer(int id) {
            this.id = id;
            d = new Date();
            email = "email_" + id + "_" + rand.nextInt(10000000) + "@domain_"
                    + rand.nextInt(10000000) + ".com";
            int maxAddr = rand.nextInt(4);
            for (int i = 0; i < maxAddr; i++)
                addresses.add(new RandomAddress(id));
        }
    }

    public static void log(String s) {
        long now = System.currentTimeMillis();
        long msToPrevLog = logPrevLog == 0 ? -1 : now - logPrevLog;
        logPrevLog = now;
        System.out.printf("[" + logDF.format(new Date(now)) + "][%1$5sms] " + s + "\n",
                msToPrevLog);
    }

    static void A1(SQLUtilInterface sql, List<RandomCustomer> customers, TestDB tstDB)
            throws SQLException, InterruptedException {
        Test.log("table init");
        tstDB.initSchema(sql, 'P');
        sql.getConnection().setAutoCommit(false);
        int custCounter = 0, addrCounter = 0;
        Test.log("SQLUtil inserting rows...");
        for (RandomCustomer cust : customers) {
            sql.executeDMLVarArgs(Test.SQL_CUST_INS, cust.id, cust.d, cust.email);
            custCounter++;
            for (RandomAddress addr : cust.addresses) {
                sql.executeDMLVarArgs(Test.SQL_ADDR_INS, addr.custId, addr.addr);
                addrCounter++;
            }
        }
        sql.commit();
        Test.log("SQLUtil inserted customers=" + custCounter + ", addresses=" + addrCounter);
    }

    static void A2(SQLUtilInterface sql, List<RandomCustomer> customers, TestDB tstDB)
            throws SQLException, InterruptedException {
        Test.log("table init");
        tstDB.initSchema(sql, 'P');
        Connection con = sql.getConnection();
        con.setAutoCommit(false);
        int custCounter = 0, addrCounter = 0;
        Test.log("JDBC inserting rows...");
        PreparedStatement psCust = null, psAddr = null;
        try {
            psCust = con.prepareStatement(Test.SQL_CUST_INS);
            psAddr = con.prepareStatement(Test.SQL_ADDR_INS);
            for (RandomCustomer cust : customers) {
                psCust.setInt(1, cust.id);
                psCust.setDate(2, new java.sql.Date(cust.d.getTime()));
                psCust.setString(3, cust.email);
                psCust.execute();
                custCounter++;
                for (RandomAddress addr : cust.addresses) {
                    psAddr.setInt(1, addr.custId);
                    psAddr.setString(2, addr.addr);
                    psAddr.execute();
                    addrCounter++;
                }
            }
        } finally {
            psCust.close();
            psAddr.close();
        }
        con.commit();
        Test.log("JDBC inserted customers=" + custCounter + ", addresses=" + addrCounter);
    }

    static void A3(SQLUtilInterface sql, List<RandomCustomer> customers, TestDB tstDB)
            throws SQLException, InterruptedException {
        Test.log("table init");
        tstDB.initSchema(sql, 'P');
        sql.getConnection().setAutoCommit(false);
        int custCounter = 0, addrCounter = 0;
        Test.log("BATCH inserting rows...");
        int[] bindTypesCust = { Types.INTEGER, Types.DATE, Types.VARCHAR };
        int[] bindTypesAddr = { Types.INTEGER, Types.VARCHAR };
        List<Object[]> batchValuesCust = new ArrayList<>();
        List<Object[]> batchValuesAddr = new ArrayList<>();
        for (RandomCustomer cust : customers) {
            Object[] c = { cust.id, new java.sql.Date(cust.d.getTime()), cust.email };
            batchValuesCust.add(c);
            for (RandomAddress addr : cust.addresses) {
                Object[] a = { addr.custId, addr.addr };
                batchValuesAddr.add(a);
            }
        }

        int[] affectedRows = null;
        affectedRows = sql.executeDMLBatch(Test.SQL_CUST_INS, batchValuesCust, bindTypesCust);
        for (int ar : affectedRows)
            custCounter += ar;
        affectedRows = sql.executeDMLBatch(Test.SQL_ADDR_INS, batchValuesAddr, bindTypesAddr);
        for (int ar : affectedRows)
            addrCounter += ar;

        sql.commit();
        Test.log("BATCH inserted customers=" + custCounter + ", addresses=" + addrCounter);
    }

    /*
    General Notes:
    Programming at home, no professional network hardware, so ping times are generally bad.
    Localhost=MacBook Pro, Intel Core i5-4288U @ 2.60 GHz (2 cores)
    Server (Oracle & postgres)=Windows 10 Pro, Intel Pentium G2020T @ 2.50GHz (2 cores)
    Oracle 18c XE, Postgres 12.1

    ;tldr;
    1) Try to reduce the amount of network calls/reduce ping time. Use JDBC-Batches if possible.
    2) No big difference between Oracle/Postgres within this setup.
    3) No big difference between SQLUtil and pure JDBC.
    4) If you have to load big-data: Only batches are a working solution. Row By Row is too slow.
    ;tldr;

    OUTPUT OF DIFFERENT EXECUTIONS:
    ==
    Program to compare different implementations for different kind of database tasks.
    Two tables are created: sqlutil_cust(2000 rows), sqlutil_addr(0..3 rows per customer).
    1) (drop/create), populate via loop(sqlutil): for all cust; create cust and create 0..2 addr rows.
    2) (drop/create), populate via loop(jdbc direct): for all cust; create cust and create 0..2 addr rows.
    3) (drop/create), populate via batch: one batch for cust, one batch for addr
    **wlan/wlan conneting to jdbc:oracle:thin:@192.168.188.72:1521/xe**
    [Mar 28, 2020 12:09:11 AM][   -1ms] table init
    [Mar 28, 2020 12:09:11 AM][  452ms] 1 inserting rows...
    [Mar 28, 2020 12:09:40 AM][28809ms] 1 inserting customers=2000, addresses=2975
    [Mar 28, 2020 12:09:40 AM][    0ms] table init
    [Mar 28, 2020 12:09:40 AM][  292ms] 2 inserting rows...
    [Mar 28, 2020 12:10:08 AM][27661ms] 2 inserting customers=2000, addresses=2975
    [Mar 28, 2020 12:10:08 AM][    1ms] table init
    [Mar 28, 2020 12:10:08 AM][  329ms] 3 inserting rows...
    [Mar 28, 2020 12:10:09 AM][  778ms] 3 inserting customers=2000, addresses=2975

    Program to compare different implementations for different kind of database tasks.
    Two tables are created: sqlutil_cust(2000 rows), sqlutil_addr(0..3 rows per customer).
    1) (drop/create), populate via loop(sqlutil): for all cust; create cust and create 0..2 addr rows.
    2) (drop/create), populate via loop(jdbc direct): for all cust; create cust and create 0..2 addr rows.
    3) (drop/create), populate via batch: one batch for cust, one batch for addr
    **wlan/wlan conneting to jdbc:postgresql://192.168.188.72/sqlutil**
    [Mar 28, 2020 12:38:26 AM][   -1ms] table init
    [Mar 28, 2020 12:38:26 AM][  131ms] 1 inserting rows...
    [Mar 28, 2020 12:38:53 AM][27395ms] 1 inserting customers=2000, addresses=2951
    [Mar 28, 2020 12:38:53 AM][    1ms] table init
    [Mar 28, 2020 12:38:53 AM][   66ms] 2 inserting rows...
    [Mar 28, 2020 12:39:20 AM][27356ms] 2 inserting customers=2000, addresses=2951
    [Mar 28, 2020 12:39:20 AM][    0ms] table init
    [Mar 28, 2020 12:39:21 AM][   52ms] 3 inserting rows...
    [Mar 28, 2020 12:39:21 AM][  531ms] 3 inserting customers=2000, addresses=2951

    Program to compare different implementations for different kind of database tasks.
    Two tables are created: sqlutil_cust(2000 rows), sqlutil_addr(0..3 rows per customer).
    1) (drop/create), populate via loop(sqlutil): for all cust; create cust and create 0..2 addr rows.
    2) (drop/create), populate via loop(jdbc direct): for all cust; create cust and create 0..2 addr rows.
    3) (drop/create), populate via batch: one batch for cust, one batch for addr
    **localhost conneting to jdbc:postgresql://localhost/sqlutil**
    [Mar 28, 2020 12:11:17 AM][   -1ms] table init
    [Mar 28, 2020 12:11:17 AM][   40ms] 1 inserting rows...
    [Mar 28, 2020 12:11:17 AM][  683ms] 1 inserting customers=2000, addresses=3022
    [Mar 28, 2020 12:11:17 AM][    1ms] table init
    [Mar 28, 2020 12:11:17 AM][    8ms] 2 inserting rows...
    [Mar 28, 2020 12:11:18 AM][  665ms] 2 inserting customers=2000, addresses=3022
    [Mar 28, 2020 12:11:18 AM][    1ms] table init
    [Mar 28, 2020 12:11:18 AM][    8ms] 3 inserting rows...
    [Mar 28, 2020 12:11:18 AM][  215ms] 3 inserting customers=2000, addresses=3022

    Program to compare different implementations for different kind of database tasks.
    Two tables are created: sqlutil_cust(2000 rows), sqlutil_addr(0..3 rows per customer).
    1) (drop/create), populate via loop(sqlutil): for all cust; create cust and create 0..2 addr rows.
    2) (drop/create), populate via loop(jdbc direct): for all cust; create cust and create 0..2 addr rows.
    3) (drop/create), populate via batch: one batch for cust, one batch for addr
    **ethernet/wlan conneting to jdbc:postgresql://192.168.188.79/sqlutil**
    [Mar 28, 2020 11:37:00 AM][   -1ms] table init
    [Mar 28, 2020 11:37:00 AM][  200ms] 1 inserting rows...
    [Mar 28, 2020 11:37:13 AM][12778ms] 1 inserting customers=2000, addresses=2978
    [Mar 28, 2020 11:37:13 AM][    1ms] table init
    [Mar 28, 2020 11:37:13 AM][   45ms] 2 inserting rows...
    [Mar 28, 2020 11:37:26 AM][12649ms] 2 inserting customers=2000, addresses=2978
    [Mar 28, 2020 11:37:26 AM][    0ms] table init
    [Mar 28, 2020 11:37:26 AM][   36ms] 3 inserting rows...
    [Mar 28, 2020 11:37:26 AM][  388ms] 3 inserting customers=2000, addresses=2978

     */
    public void testPerformance(SQLUtilInterface sql, TestDB tstDB)
            throws SQLException, InterruptedException {
        List<RandomCustomer> customers = new ArrayList<>();
        for (int i = 1; i <= Test.ROWSCREATE; i++)
            customers.add(new RandomCustomer(i));

        try {
            // sql.getAudit().enable(true);

            for (int i = 0; i < 2; i++) {
                Test.A1(sql, customers, tstDB);
                Test.A2(sql, customers, tstDB);
                Test.A3(sql, customers, tstDB);
            }

            // sql.getAudit().printSummary(System.out, 0, null);
        } finally {
            if (sql != null)
                sql.closeConnection();
        }

    }

}
