/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/SQLUtil
 */
package com.cg.sqlutil.test;

import java.sql.SQLException;

import com.cg.sqlutil.SQLUtilInterface;

public interface TestDB {
    void initSchema(SQLUtilInterface sql, char mode) throws SQLException;

    void testRollback(SQLUtilInterface sql) throws SQLException;

    void testString(SQLUtilInterface sql) throws SQLException;

    void testLong(SQLUtilInterface sql) throws SQLException;

    void testBigDecimal(SQLUtilInterface sql) throws SQLException;

    void testTimestamp(SQLUtilInterface sql) throws SQLException;

    void testRaw(SQLUtilInterface sql) throws SQLException;
}
