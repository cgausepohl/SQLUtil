/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/SQLUtil
 */
package com.cg.sqlutil.exception;

@SuppressWarnings("serial")
public class ReturnedMoreThanOneRowException extends SingleRowQueryException {

    public ReturnedMoreThanOneRowException() {
        this("single row query returned more than one row");
    }

    public ReturnedMoreThanOneRowException(String reason) {
        super(reason);
    }

    public ReturnedMoreThanOneRowException(Throwable cause) {
        super(cause);
    }

    public ReturnedMoreThanOneRowException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public ReturnedMoreThanOneRowException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public ReturnedMoreThanOneRowException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public ReturnedMoreThanOneRowException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
    }

    public ReturnedMoreThanOneRowException(String reason, String sqlState, int vendorCode,
            Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
    }

}
