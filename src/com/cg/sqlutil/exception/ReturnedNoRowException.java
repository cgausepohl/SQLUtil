/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/SQLUtil
 */
package com.cg.sqlutil.exception;

@SuppressWarnings("serial")
public class ReturnedNoRowException extends SingleRowQueryException {

    public ReturnedNoRowException() {
        super();
    }

    public ReturnedNoRowException(String reason) {
        super(reason);
    }

    public ReturnedNoRowException(Throwable cause) {
        super(cause);
    }

    public ReturnedNoRowException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public ReturnedNoRowException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public ReturnedNoRowException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public ReturnedNoRowException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
    }

    public ReturnedNoRowException(String reason, String sqlState, int vendorCode, Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
    }

}
