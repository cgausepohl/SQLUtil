package com.binrock.sqlutil.exception;

import java.sql.SQLException;

@SuppressWarnings("serial")
public abstract class SingleRowQueryException extends SQLException {

	public SingleRowQueryException() {
	}

	public SingleRowQueryException(String reason) {
		super(reason);
	}

	public SingleRowQueryException(Throwable cause) {
		super(cause);
	}

	public SingleRowQueryException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public SingleRowQueryException(String reason, Throwable cause) {
		super(reason, cause);
	}

	public SingleRowQueryException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

	public SingleRowQueryException(String reason, String sqlState, Throwable cause) {
		super(reason, sqlState, cause);
	}

	public SingleRowQueryException(String reason, String sqlState, int vendorCode, Throwable cause) {
		super(reason, sqlState, vendorCode, cause);
	}

}
