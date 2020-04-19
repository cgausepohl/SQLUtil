package com.binrock.sqlutil.impl;

import java.io.PrintStream;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import com.binrock.sqlutil.AuditInterface;
import com.binrock.sqlutil.AuditRecord;
import com.binrock.sqlutil.SQLUtilFactory;
import com.binrock.sqlutil.SQLUtilInterface;
import com.binrock.sqlutil.SQLUtilInterface.DBProduct;

public class Audit implements AuditInterface {

    private boolean enabled = true;
    private Hashtable<String, ArrayList<AuditRecord>> auditRecords = new Hashtable<>();
    private List<String> sqlPatternsForBindValueMasking = new LinkedList<>();
    private String insertStmtSQL = INSERT_STMT_SQL_DEFAULT, insertStmtExecution = INSERT_STMT_EXECUTION_DEFAULT;
    private String conJdbc, conUser, conPassword;
    private AuditRecord currentAuditRecord;

    public Audit() {
    }

    @Override
    public void enable(boolean enable) {
        this.enabled = enable;
        if (!enable)
            currentAuditRecord = null;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public Hashtable<String, ArrayList<AuditRecord>> getAuditRecords() {
        return auditRecords;
    }

    @Override
    public void flush() {
        getAuditRecords().clear();
    }

    @Override
    public void flush(long olderThanSec) {
        // TODO implement second logic
        flush();
    }

    // level 0: all executions, 1: grouped by sql+bindvars, 2: grouped by sql, 3: top10-all-time, 4:top10-last24h, 5: top10-last1h
    // sqlContains: if !=null only summarize where sql contains sqlContains (case insensitive)
    @Override
    public void printSummary(PrintStream out, int level, String sqlContains) {
        if (!enabled) return;
        if (level==0) {
            getAuditRecords().forEach((sql, execs) -> {
                System.out.println("START:");
                System.out.println(sql);
                int i=0;
                for (AuditRecord stat: execs) {
                    System.out.println("  "+(i++)+":"+stat.toString());
                }
            });
            return;
        }
        throw new IllegalArgumentException("level not implmented:"+level);
    }

    @Override
    public void setConnectionData(String jdbc, String user, String password) {
        this.conJdbc = jdbc;
        this.conUser = user;
        this.conPassword = password;
    }

    @Override
    // you should try batch mode. if it doesnt work for your environment you can switch to single stmt (slower)
    public void store(UseCurrentThread ownThread, String appId, StoreBindVariables storeBindValues,
            UseBatchInserts batched) throws SQLException {
        if (!enabled) return;
        if (UseCurrentThread.NO==ownThread)
            throw new UnsupportedOperationException("UseCurrentThread.NO not implemented");
        SQLUtilInterface sql = null;
        try {
            sql = SQLUtilFactory.createSQLUtil(conJdbc, conUser, conPassword);
            if (sql.getDBProduct()!=DBProduct.POSTGRESQL) {
                System.out.println("only postgres supported as audit target. current dbproduct="+sql.getDBProduct());
                return;
            }
            sql.getAudit().enable(false);
            sql.getConnection().setAutoCommit(false);

            // Postgres
            sql.executeDDL("create table if not exists sqlutil_audit_sql (sql_id numeric(18,0) not null primary key,appid varchar(100) not null,created timestamp not null,sql_text text not null)");
            sql.executeDDL("create table if not exists sqlutil_audit_execution (sql_id numeric(18,0) not null references sqlutil_audit_sql(sql_id),started timestamp not null,duration_ms numeric(18,3),rowsaffected numeric(18), err varchar(1000),bindvalues text)");

            // Set Ids
            int[] bindTypesSQL = {Types.BIGINT, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR};
            int[] bindTypesExecution = {Types.BIGINT, Types.TIMESTAMP, Types.REAL, Types.INTEGER, Types.VARCHAR, Types.VARCHAR};

            if (UseBatchInserts.NO==batched) {
                Enumeration<String> sqls = getAuditRecords().keys();
                while (sqls.hasMoreElements()) {
                    // "insert into sqlutil_audit_sql(sql_id,appid,created,sql_text)values(?,?,?,?)"
                    String sqlText = sqls.nextElement();
                    Long sqlId = new Long(System.nanoTime());
                    Object[] bindValuesSQL = {sqlId, appId, new Timestamp(System.currentTimeMillis()), sqlText};
                    sql.executeDML(insertStmtSQL, bindValuesSQL, bindTypesSQL);
                    ArrayList<AuditRecord> recs = getAuditRecords().get(sqlText);
                    for (int i=0; i<recs.size(); i++) {
                        AuditRecord ar = recs.get(i);
                        // "insert into sqlutil_audit_execution(execution_id,sql_id,started,duration_ms,err,bindvalues)values(?,?,?,?,?,?)";
                        String err=ar.getError()!=null?ar.getError().getMessage():null;
                        String bindVar = getBindVarValue(ar,storeBindValues);
                        Object[] bindVariables =
                            {sqlId,ar.getStarted(),ar.getDurationMs(),ar.getRowsAffected(),err,bindVar};
                        sql.executeDML(insertStmtExecution, bindVariables, bindTypesExecution);
                        recs.set(i, null);
                    }
                }
            } else {
                List<Object[]> bindValuesSQL = new ArrayList<>();
                List<Object[]> bindValuesExecute = new ArrayList<>();
                Enumeration<String> sqls = getAuditRecords().keys();
                while (sqls.hasMoreElements()) {
                    String sqlText = sqls.nextElement();
                    Long sqlId = new Long(System.nanoTime());
                    Object[] rowInsertSQL = {sqlId, appId, new Timestamp(System.currentTimeMillis()), sqlText};
                    bindValuesSQL.add(rowInsertSQL);
                    ArrayList<AuditRecord> recs = getAuditRecords().get(sqlText);
                    for (AuditRecord ar:recs) {
                        String err=ar.getError()!=null?ar.getError().getMessage():null;
                        String bindVar = getBindVarValue(ar,storeBindValues);
                        Object[] rowInsertExec = {sqlId,ar.getStarted(),ar.getDurationMs(),ar.getRowsAffected(),err,bindVar};
                        bindValuesExecute.add(rowInsertExec);
                    }
                }
                sql.executeDMLBatch(insertStmtSQL, bindValuesSQL, bindTypesSQL);
                sql.executeDMLBatch(insertStmtExecution, bindValuesExecute, bindTypesExecution);
            }
            sql.commit();

            // remove all stored entries
            cleanupAuditRecords();

        } finally {
            if (sql!=null)
                sql.closeConnection();
        }
    }

    private String getBindVarValue(AuditRecord r, StoreBindVariables store) {
        if (StoreBindVariables.NO==store) return null;
        if (sqlPatternsForBindValueMasking==null) return null;
        if (sqlPatternsForBindValueMasking.size()==0) return null;
        for (String substr: sqlPatternsForBindValueMasking)
            if (r.getSQL().indexOf(substr)>0)
                return null;
        return r.getBindValues();
    }

    // todo: is this really correct?
    private synchronized void cleanupAuditRecords() {
        Hashtable<String, ArrayList<AuditRecord>> hashRecs = getAuditRecords();
        synchronized (hashRecs) {
            ArrayList<String> keysToDel = new ArrayList<>();
            Enumeration<String> sqls = hashRecs.keys();
            while (sqls.hasMoreElements()) {
                String sqlText = sqls.nextElement();
                ArrayList<AuditRecord> recs = hashRecs.get(sqlText);
                recs.removeIf(n -> (n==null));
                if (recs.size()==0)
                    keysToDel.add(sqlText);
            }
            for (String keyToDel: keysToDel)
                hashRecs.remove(keyToDel);
        }
    }

    @Override
    public void setInsertStmtForSQL(String sql) {
        insertStmtSQL = sql;
    }

    @Override
    public void setInsertStmtForExecution(String sql) {
        insertStmtExecution = sql;
    }

    @Override
    public List<String> getListPatternsForBindValueMasking() {
        return sqlPatternsForBindValueMasking;
    }

    @Override
    public void startNewAuditRecord(String sql, Object[] bindVariables) {
        if (!enabled) return;
        currentAuditRecord = new AuditRecord(sql, bindVariables);
    }

    @Override
    public void startNewAuditRecord(String sql) {
        if (!enabled) return;
        startNewAuditRecord(sql, null);
    }

    @Override
    public void endAuditRecord(int rowsaffected) {
        if (!enabled) return;
        currentAuditRecord.finish(rowsaffected);
        addCurrentRecord();
    }

    @Override
    public void endAuditRecord() {
        if (!enabled) return;
        currentAuditRecord.finish();
        addCurrentRecord();
    }

    @Override
    public void endAuditRecord(Throwable t) {
        if (!enabled) return;
        currentAuditRecord.finish(t);
        addCurrentRecord();
    }

    private void addCurrentRecord() {
        if (!enabled) return;
        String sql = currentAuditRecord.getSQL();
        ArrayList<AuditRecord> recs = getAuditRecords().get(sql);
        if (recs==null) {
            recs = new ArrayList<>();
            recs.add(currentAuditRecord);
            getAuditRecords().put(sql, recs);
        } else {
            recs.add(currentAuditRecord);
        }
    }

}


/*
    @Override
    public void enableTimings(boolean enable) {
        if (enable) {
            if (statisticsExecutionTiming==null)
                statisticsExecutionTiming = new Hashtable<>();
        } else {
            if (statisticsExecutionTiming!=null)
                statisticsExecutionTiming.clear();
            statisticsExecutionTiming = null;
        }
    }

    @Override
    public Hashtable<String, List<AuditRecord>> getExecutionStatistcs() {
        return statisticsExecutionTiming;
    }

    @Override
    public void flushExecutionStatistcs() {
        statisticsExecutionTiming.clear();
    }

    @Override
    public void flushExecutionStatistcs(long olderThanSec) {
        throw new NotImplementedException();
    }
    //STAT
    private Hashtable<String, List<AuditRecord>> statisticsExecutionTiming = null;

    private void addExecutionStatistic(String sql, AuditRecord stat) {
        if (statisticsExecutionTiming==null) return;
        List<AuditRecord> stats = statisticsExecutionTiming.get(sql);
        if (stats==null) {
            stats = new ArrayList<>();
            stats.add(stat);
            statisticsExecutionTiming.put(sql, stats);
        } else {
            stats.add(stat);
        }
    }

 */
