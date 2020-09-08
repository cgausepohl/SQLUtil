/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/SQLUtil
 */
package com.cg.sqlutil.impl;

import java.sql.SQLException;

import com.cg.sqlutil.Row;
import com.cg.sqlutil.SQLUtilInterface;

class Query {

    private SQLUtilInterface sql;
    private String selectStmt;
    private Row[] rows;

    public Query(SQLUtilInterface sql, final String selectStmt) throws SQLException {
        this.sql = sql;
        this.selectStmt = selectStmt;
    }

    public Query execute() throws SQLException {
        return execute(null, null);
    }

    public Query execute(final Object[] bindVariables) throws SQLException {
        return execute(bindVariables, null);
    }

    public Query executeVarArgs(final Object... bindVariables) throws SQLException {
        return execute(bindVariables, null);
    }

    public Query execute(final Object[] bindVariables, final int[] bindTypes) throws SQLException {
        rows = sql.getRows(selectStmt, bindVariables, bindTypes);
        return this;
    }

    public Row[] getRows() {
        return rows;
    }

}