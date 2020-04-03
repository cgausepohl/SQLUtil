package com.binrock.sqlutil.examples;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.binrock.sqlutil.SQLUtilFactory;
import com.binrock.sqlutil.SQLUtilInterface;

public class Comparison {

    static final int CUST_ROWS=10;
    static final int DB_ORA=1, DB_PGSQL=2;
    static int CURRENTDB = DB_PGSQL;
    static final String SQL_CUST_INS = "insert into sqlutil_cust(cust_id,d,email)values(?,?,?)";
    static final String SQL_ADDR_INS = "insert into sqlutil_addr(cust_id,addr)values(?,?)";

    static long logPrevLog=0;
    static DateFormat logDF = DateFormat.getDateTimeInstance();
    static Random rand = new Random();

    static class RandomAddress {
        static final String CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";
        private int custId;
        private String addr;
        RandomAddress(int custId) {
            this.custId=custId;
            StringBuilder sb = new StringBuilder(50);
            int l=CHARS.length();
            for (int i = 0; i < 50; i++)
                sb.append(CHARS.charAt(rand.nextInt(l)));
            addr = sb.toString();
        }
    }

    static class RandomCustomer {
        private int id;
        private Date d;
        private String email;
        List<RandomAddress> addresses = new ArrayList<>();
        RandomCustomer(int id) {
            this.id = id;
            d = new Date();
            email = "email_"+id+"_"+rand.nextInt(10000000)+"@domain_"+rand.nextInt(10000000)+".com";
            int maxAddr = rand.nextInt(4);
            for (int i=0; i<maxAddr; i++)
                addresses.add(new RandomAddress(id));
        }
    }

    static void log(String s) {
        long now = System.currentTimeMillis();
        long msToPrevLog = logPrevLog==0?-1:now-logPrevLog;
        logPrevLog = now;
        System.out.printf("["+logDF.format(new Date(now))+"][%1$5sms] "+s+"\n", msToPrevLog);
    }

    static void createTableExample(SQLUtilInterface sql) throws SQLException {
        String [] cmds = null;
        String [] oraCmds = {"DROP TABLE sqlutil_cust CASCADE constraints",
                    "DROP TABLE sqlutil_addr CASCADE constraints",
                    "DROP SEQUENCE seq_sqlutil_addr",
                    "create table sqlutil_cust(cust_id int primary key, d date, email varchar2(100))",
                    "create table sqlutil_addr(addr_id int primary key, cust_id int references sqlutil_cust, addr varchar(200))",
                    "CREATE SEQUENCE seq_sqlutil_addr",
                    "begin execute immediate 'CREATE OR REPLACE TRIGGER trg_sqlutiladdr_pk BEFORE INSERT ON sqlutil_addr FOR EACH ROW "+
                      "BEGIN :new.addr_id := seq_sqlutil_addr.nextval; END;'; end;"};
        String [] pgCmds = {"drop table if exists sqlutil_addr",
                    "drop table if exists sqlutil_cust",
                    "create table sqlutil_cust(cust_id int primary key, d date, email varchar(100))",
                    "create table sqlutil_addr(addr_id serial primary key, cust_id int references sqlutil_cust, addr varchar(200))"};

        if (CURRENTDB==DB_ORA) {
            cmds = oraCmds;
        } else if (CURRENTDB==DB_PGSQL) {
            cmds = pgCmds;
        }
        for (String cmd: cmds) {
            try {
                sql.executeDDL(cmd);
            } catch (SQLException ignore) {System.out.println("Error:"+ignore.getMessage()+", stmt="+cmd);}
        }
    }


    static void A1(SQLUtilInterface sql, List<RandomCustomer> customers) throws SQLException, InterruptedException {
        log("table init");
        createTableExample(sql);
        sql.getConnection().setAutoCommit(false);
        int custCounter=0, addrCounter=0;
        log("SQLUtil inserting rows...");
        for (RandomCustomer cust: customers) {
            sql.executeDMLVarArgs(SQL_CUST_INS, cust.id, cust.d, cust.email);
            custCounter++;
            for (RandomAddress addr: cust.addresses) {
                sql.executeDMLVarArgs(SQL_ADDR_INS, addr.custId, addr.addr);
                addrCounter++;
            }
        }
        sql.commit();
        log("SQLUtil inserting customers="+custCounter+", addresses="+addrCounter);
    }

    static void A2(SQLUtilInterface sql, List<RandomCustomer> customers) throws SQLException, InterruptedException {
        log("table init");
        createTableExample(sql);
        Connection con = sql.getConnection();
        con.setAutoCommit(false);
        int custCounter=0, addrCounter=0;
        log("JDBC inserting rows...");
        PreparedStatement psCust = null, psAddr=null;
        try {
            psCust = con.prepareStatement(SQL_CUST_INS);
            psAddr = con.prepareStatement(SQL_ADDR_INS);
            for (RandomCustomer cust: customers) {
                psCust.setInt(1, cust.id);
                psCust.setDate(2, new java.sql.Date(cust.d.getTime()));
                psCust.setString(3, cust.email);
                psCust.execute();
                custCounter++;
                for (RandomAddress addr: cust.addresses) {
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
        log("JDBC inserting customers="+custCounter+", addresses="+addrCounter);

    }

    static void A3(SQLUtilInterface sql, List<RandomCustomer> customers) throws SQLException, InterruptedException {
        log("table init");
        createTableExample(sql);
        sql.getConnection().setAutoCommit(false);
        int custCounter=0, addrCounter=0;
        log("BATCH inserting rows...");
        int[] bindTypesCust = {Types.INTEGER, Types.DATE, Types.VARCHAR};
        int[] bindTypesAddr = {Types.INTEGER, Types.VARCHAR};
        List<Object[]> batchValuesCust = new ArrayList<>();
        List<Object[]> batchValuesAddr = new ArrayList<>();
        for (RandomCustomer cust: customers) {
            Object[] c = {cust.id, new java.sql.Date(cust.d.getTime()), cust.email};
            batchValuesCust.add(c);
            for (RandomAddress addr: cust.addresses) {
                Object[] a = {addr.custId, addr.addr};
                batchValuesAddr.add(a);
            }
        }

        int[] affectedRows = null;
        affectedRows = sql.executeDMLBatch(SQL_CUST_INS, batchValuesCust, bindTypesCust);
        for (int ar: affectedRows)
            custCounter+=ar;
        affectedRows = sql.executeDMLBatch(SQL_ADDR_INS, batchValuesAddr, bindTypesAddr);
        for (int ar: affectedRows)
            addrCounter+=ar;

        sql.commit();
        log("BATCH inserting customers="+custCounter+", addresses="+addrCounter);
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        System.out.println("Program to compare different implementations for different kind of database tasks.");
        System.out.println("Two tables are created: sqlutil_cust("+CUST_ROWS+" rows), sqlutil_addr(0..3 rows per customer).");
        System.out.println("1) (drop/create), populate via loop(sqlutil): for all cust; create cust and create 0..2 addr rows.");
        System.out.println("2) (drop/create), populate via loop(jdbc direct): for all cust; create cust and create 0..2 addr rows.");
        System.out.println("3) (drop/create), populate via batch: one batch for cust, one batch for addr");

        List<RandomCustomer> customers = new ArrayList<>();
        for (int i=1; i<=CUST_ROWS; i++)
            customers.add(new RandomCustomer(i));

        SQLUtilInterface sql = null;
        try {
            String jdbc = null;
            if (CURRENTDB==DB_PGSQL) {
                // localhost
                jdbc = "jdbc:postgresql://localhost/sqlutil";
                // wlan/wlan
                // jdbc = "jdbc:postgresql://192.168.188.72/sqlutil";
                // ethernet/wlan
                // jdbc = "jdbc:postgresql://192.168.188.79/sqlutil";
            } else if (CURRENTDB==DB_ORA) {
                jdbc = "jdbc:oracle:thin:@192.168.188.72:1521/xe";
            }
            System.out.println("conneting to jdbc:"+jdbc);
            sql = SQLUtilFactory.createSQLUtil(jdbc, "sqlutil", "sqlutil");
            sql.getAudit().enable(true);

            for (int i=0;i<2;i++) {
                sql.getString("select 'bla'");
                A1(sql, customers);
                A2(sql, customers);
                A3(sql, customers);
            }

            sql.getAudit().printSummary(System.out, 0, null);

        } finally {
            if (sql!=null)
                sql.closeConnection();
        }
        log("done");
    }
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
