package com.binrock.sqlutil.exception;

import java.sql.SQLException;

public class SQLSingleRowQueryReturnedMoreThanOneRowsException extends SQLException {

	private static final long serialVersionUID = 1L;

	public SQLSingleRowQueryReturnedMoreThanOneRowsException() {
		this("single row query returned more than one row");
	}

	public SQLSingleRowQueryReturnedMoreThanOneRowsException(String reason) {
		super(reason);
	}

	public SQLSingleRowQueryReturnedMoreThanOneRowsException(Throwable cause) {
		super(cause);
	}

	public SQLSingleRowQueryReturnedMoreThanOneRowsException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public SQLSingleRowQueryReturnedMoreThanOneRowsException(String reason, Throwable cause) {
		super(reason, cause);
	}

	public SQLSingleRowQueryReturnedMoreThanOneRowsException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

	public SQLSingleRowQueryReturnedMoreThanOneRowsException(String reason, String sqlState, Throwable cause) {
		super(reason, sqlState, cause);
	}

	public SQLSingleRowQueryReturnedMoreThanOneRowsException(String reason, String sqlState, int vendorCode,
			Throwable cause) {
		super(reason, sqlState, vendorCode, cause);
	}

}
