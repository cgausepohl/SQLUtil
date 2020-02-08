package com.binrock.sqlutil.exception;

import java.sql.SQLException;

public class SQLSingleRowQueryReturnedNoRowException extends SQLException {

	private static final long serialVersionUID = 1L;

	public SQLSingleRowQueryReturnedNoRowException() {
		super();
	}

	public SQLSingleRowQueryReturnedNoRowException(String reason) {
		super(reason);
	}

	public SQLSingleRowQueryReturnedNoRowException(Throwable cause) {
		super(cause);
	}

	public SQLSingleRowQueryReturnedNoRowException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public SQLSingleRowQueryReturnedNoRowException(String reason, Throwable cause) {
		super(reason, cause);
	}

	public SQLSingleRowQueryReturnedNoRowException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

	public SQLSingleRowQueryReturnedNoRowException(String reason, String sqlState, Throwable cause) {
		super(reason, sqlState, cause);
	}

	public SQLSingleRowQueryReturnedNoRowException(String reason, String sqlState, int vendorCode, Throwable cause) {
		super(reason, sqlState, vendorCode, cause);
	}

}
