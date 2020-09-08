/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/SQLUtil
 */
package com.cg.sqlutil;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public abstract interface AuditInterface {
    // starts or stops auditing. stop will not flush existing audit.
    void enable(boolean enable);

    boolean isEnabled();

    Hashtable<String, ArrayList<AuditRecord>> getAuditRecords();

    void flush();

    void flush(long olderThanSec);

    // level 0: all executions, 1: grouped by sql+bindvars, 2: grouped by sql, 3: top10-all-time, 4:top10-last24h, 5: top10-last1h
    // sqlContains: if !=null only summarize where sql contains sqlContains (case insensitive)
    void printSummary(PrintStream out, int level, String sqlContains);

    // do audit
    void startNewAuditRecord(String sql);

    void startNewAuditRecord(String sql, Object[] bindVariables);

    void endAuditRecord(int rowsaffected);

    void endAuditRecord(Throwable t);

    void endAuditRecord();

    //
    // store into database and remove audit records from memory
    //
    // connection only active during store. It'll be closed at the end of store(..)
    void setConnectionData(String jdbc, String user, String passwd);

    public enum UseCurrentThread {
        YES, NO
    }

    public enum UseBatchInserts {
        YES, NO
    }

    public enum StoreBindVariables {
        YES, NO
    }

    void store(UseCurrentThread ownThread, String appId,
            StoreBindVariables storeBindValues, UseBatchInserts batched) throws SQLException;

    String INSERT_STMT_SQL_DEFAULT = "insert into sqlutil_audit_sql(sql_id,appid,created,sql_text)values(?,?,?,?)";

    void setInsertStmtForSQL(String sql);

    String INSERT_STMT_EXECUTION_DEFAULT = "insert into sqlutil_audit_execution(sql_id,started,duration_ms,rowsaffected,err,bindvalues)values(?,?,?,?,?,?)";

    void setInsertStmtForExecution(String sql);

    // if storing bindVariables=YES, you shouldn't store sensible data in audit.
    // you can add a list of exclusions. a stmt is excluded if one of the sqlContainsIgnoreCase is
    // part of the sql.
    List<String> getListPatternsForBindValueMasking();
}
