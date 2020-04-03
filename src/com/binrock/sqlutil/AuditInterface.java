package com.binrock.sqlutil;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public abstract interface AuditInterface {
    // starts or stops auditing. stop will not flush existing audit.
    public void enable(boolean enable);
    public boolean isEnabled();
    public Hashtable<String, ArrayList<AuditRecord>> getAuditRecords();
    public void flush();
    public void flush(long olderThanSec);
    // level 0: all executions, 1: grouped by sql+bindvars, 2: grouped by sql, 3: top10-all-time, 4:top10-last24h, 5: top10-last1h
    // sqlContains: if !=null only summarize where sql contains sqlContains (case insensitive)
    public void printSummary(PrintStream out, int level, String sqlContains);

    // do audit
    public void startNewAuditRecord(String sql);
    public void startNewAuditRecord(String sql, Object[] bindVariables);
    public void endAuditRecord(int rowsaffected);
    public void endAuditRecord(Throwable t);
    public void endAuditRecord();

    //
    // store into database and remove audit records from memory
    //
    // connection only active during store. It'll be closed at the end of store(..)
    public void setConnectionData(String jdbc, String user, String passwd);
    public enum UseCurrentThread {YES, NO};
    public enum UseBatchInserts {YES, NO};
    public enum StoreBindVariables {YES, NO};
    public abstract void store(UseCurrentThread ownThread, String appId, StoreBindVariables storeBindValues, UseBatchInserts batched) throws SQLException;
    public static final String INSERT_STMT_SQL_DEFAULT =
            "insert into sqlutil_audit_sql(sql_id,appid,created,sql_text)values(?,?,?,?)";
    public void setInsertStmtForSQL(String sql);
    public static final String INSERT_STMT_EXECUTION_DEFAULT =
            "insert into sqlutil_audit_execution(sql_id,started,duration_ms,rowsaffected,err,bindvalues)values(?,?,?,?,?,?)";
    public void setInsertStmtForExecution(String sql);
    // if storing bindVariables=YES, you shouldn't store sensible data in audit.
    // you can add a list of exclusions. a stmt is excluded if one of the sqlContainsIgnoreCase is
    // part of the sql.
    public List<String> getListPatternsForBindValueMasking();
}
