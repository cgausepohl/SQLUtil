package com.binrock.sqlutil.test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.Random;

import com.binrock.sqlutil.AuditInterface.StoreBindVariables;
import com.binrock.sqlutil.AuditInterface.UseBatchInserts;
import com.binrock.sqlutil.AuditInterface.UseCurrentThread;
import com.binrock.sqlutil.Row;
import com.binrock.sqlutil.SQLUtilFactory;
import com.binrock.sqlutil.SQLUtilInterface;

public class Test {

	private static Random rand = new Random();
	private static SQLUtilInterface sql;
	private static long ROWSCREATE = 1000, EXECSELECTS = 2;

	// expecting a postgres-db with user sqlutil and password SQLutil$1
	// tables and data will be set up during this test
	public static void main(String[] args) throws SQLException {
		//printPublicMembers();
		System.out.println("starting");
		long programStarted = System.currentTimeMillis();
		try {
			try {
				sql = SQLUtilFactory.createSQLUtil(args[0], args[1], args[2],
						  SQLUtilFactory.RWMODE_READWRITE, SQLUtilFactory.AUTOCOMMIT_ON, Connection.TRANSACTION_READ_UNCOMMITTED);
			} catch (Exception e) {
				System.out.println("postgres db and user not valid. Install postgres and execute 'createdb sqlutil(return)' and 'createuser sqlutil -P (return and enter SQLutil$1 as password)");
				throw e;
			}

			sql.executeDDL("drop table if exists sqlutil_data");

			sql.executeDDL(
					"create table sqlutil_data (v_integer integer,v_smallint smallint,v_int int"
					+ ",v_serial serial,v_numeric18 numeric(18),v_numeric38 numeric(38)"
					+ ",v_numeric18_5 numeric(18,5),v_float7 float(7),v_float8 float8,v_real real"
					+ ",v_date date,v_time time,v_timestamp timestamp,v_timestamptz timestamptz"
					+ ",v_boolean boolean,v_bytea bytea,v_varchar100 varchar(100),v_char10 char(10)"
					+ ",v_text text)");
			System.out.println("fill table, " + ROWSCREATE + " rows...");
			Object[] allNulls = new Object[18];
			int[] allTypes = { Types.INTEGER, Types.SMALLINT, Types.INTEGER, Types.NUMERIC, Types.DECIMAL,
					Types.DECIMAL, Types.DOUBLE, Types.DOUBLE, Types.FLOAT, Types.DATE, Types.TIME, Types.TIMESTAMP,
					Types.TIMESTAMP_WITH_TIMEZONE, Types.BOOLEAN, Types.VARBINARY, Types.VARCHAR, Types.VARCHAR,
					Types.VARCHAR };
			for (int i = 1; i <= ROWSCREATE; i++) {
				String insertStmt = "insert into sqlutil_data("
						+ "v_integer, v_smallint, v_int, v_numeric18, v_numeric38,"
						+ "v_numeric18_5, v_float7,v_float8, v_real," + "v_date, v_time, v_timestamp, v_timestamptz,"
						+ "v_boolean, v_bytea," + "v_varchar100, v_char10, v_text"
						+ ") values(/*ints*/?,?,?,?,?,  /*floats*/?,?,?,?,  /*dates*/?,?,?,?,  /*bool,bytea*/?,?, /*strings*/?,?,?)";
				if (i % 2 == 0) {
					sql.executeDMLVarArgs(insertStmt, new Integer(i), new Integer(i), new Integer(i), new BigDecimal(i),
							new BigDecimal(i), new Float(1), new Double(2), new BigDecimal(4), new Float(4), new Date(),
							new java.sql.Time(new Date().getTime()), new java.sql.Timestamp(new Date().getTime()),
							new java.sql.Timestamp(new Date().getTime()), Boolean.TRUE, bytea(), weirdString(i, 30),
							weirdString(i, 10), weirdString(i, 5000));
				} else {
					sql.executeDML(insertStmt, allNulls, allTypes);
				}
			}
			sql.commit();
			// just to have it called once
			sql.rollback();

			// get counts with different types
			System.out.println("get counts with different types, " + EXECSELECTS + " times...");
			for (int i = 0; i < EXECSELECTS; i++) {
				long l = sql.getLong("select count(*) from sqlutil_data");
				test(l == ROWSCREATE, "init test 1");
				BigDecimal bd = sql.getBigDecimal("select count(*) from sqlutil_data");
				test(new BigDecimal(ROWSCREATE).equals(bd), "init test 2");
				String str = sql.getString("select cast(count(*)as text) from sqlutil_data");
				test(new Long(ROWSCREATE).toString().equals(str), "init test 3");
			}
			Row[] fullData = sql.getRows("select v_integer,v_smallint,v_int,v_serial,v_numeric18,v_numeric38,"// 1
					+ "v_numeric18_5,v_float7,v_float8,v_real,"// 7
					+ "v_date,v_time,v_timestamp,v_timestamptz,"// 11
					+ "v_boolean,"// 15
					+ "v_varchar100,v_char10,v_text from sqlutil_data order by 1");// 16
			test(fullData.length == ROWSCREATE, "test5");

			// ALL TESTS
			testString();
			testLong();
			testBigDecimal();
			testTimestamp();
			testRaw();
			testStream();

			sql.getAudit().setConnectionData(args[0], args[1], args[2]);
			sql.getAudit().store(UseCurrentThread.YES, Test.class.getSimpleName(), StoreBindVariables.YES, UseBatchInserts.YES);
		} finally {
			if (sql != null)
				sql.closeConnection();
			//sql.getAudit().printSummary(System.out,  0, null);
		}

		System.out.println("done, exec time(ms)=" + (System.currentTimeMillis() - programStarted));
	}

	private static void testStream() throws SQLException {
			sql.getChunksPrepare("select v_integer from sqlutil_data", (int)(ROWSCREATE/20));
    		Row[] rows = null;
        	long sum=0, rowcount=0;
    		while (true) {
        		rows=sql.getChunksGetNextRows();
        		if (rows==null) break;
        		rowcount+=rows.length;
        		for (Row row:rows)
        			if (row!=null && row.getLong(0)!=null)
        				sum += row.getLong(0);
        	}
        	long cntAll = sql.getLong("select sum(v_integer) from sqlutil_data");
        	test((cntAll-sum)==0, "batch mode, getting sum(v_integer)");
        	test((rowcount-ROWSCREATE)==0, "batch mode, reading rows");
	}

	public static void printPublicMembers() {
		Class<Types> c = Types.class;
		for (Field f : c.getDeclaredFields()) {
			int mod = f.getModifiers();
			if (Modifier.isStatic(mod) && Modifier.isPublic(mod) && Modifier.isFinal(mod)) {
				try {
					System.out.printf("%s = %d%n", f.getName(), f.get(null));
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void testTimestamp() throws SQLException {
		System.out.println("test getTimestamp* methods");
		String col = "v_timestamp";
		String typ = "Timestamp";
		Timestamp l = sql
				.getTimestamp("select " + col + " from sqlutil_data where " + col + " is null limit 1");
		test(l == null, "get" + typ + " test 1");
		l = sql.getTimestamp("select " + col + " from sqlutil_data where " + col + " is not null limit 1");
		test(l != null, "get" + typ + " test 2");
		l = sql.getTimestampVarArgs("select " + col + " from sqlutil_data where " + col + " is null limit ?",
				new Integer(1));
		test(l == null, "get" + typ + " v_integer 3");
		l = sql.getTimestampVarArgs("select " + col + " from sqlutil_data where " + col + " is not null limit ?",
				new Integer(1));
		test(l != null, "get" + typ + " test 4");

		Object[] bindVars = { new Integer(1) };
		l = sql.getTimestamp("select " + col + " from sqlutil_data where " + col + " is null limit ?", bindVars);
		test(l == null, "get" + typ + " test 5");
		l = sql.getTimestamp("select " + col + " from sqlutil_data where " + col + " is not null limit ?", bindVars);
		test(l != null, "get" + typ + " test 6");

		Object[] bindVars2a = { null, null, new Integer(1) };
		int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
		l = sql.getTimestamp("select " + col + " from sqlutil_data where (? is null or ?=v_serial) limit ?", bindVars2a,
				sqlTypes2);
		test(l == null, "get" + typ + " test 7");
		Object[] bindVars2b = { new Integer(1), new Integer(2), new Integer(1) };
		l = sql.getTimestamp("select " + col + " from sqlutil_data where (? is null or ?=v_serial) limit ?", bindVars2b,
				sqlTypes2);
		test(l != null, "get" + typ + " test 8");

		// getBigDecimals
		Timestamp[] ll = sql
				.getTimestamps("select " + col + " from sqlutil_data where " + col + " is null limit 100");
		test(ll != null && ll.length == 100, "get" + typ + "s test 1");
		ll = sql.getTimestampsVarArgs("select " + col + " from sqlutil_data where v_serial>? limit 100",
				new Integer(0));
		test(ll != null && ll.length == 100, "get" + typ + "s test 2");
		Object[] bindVars3 = { new Integer(0) };
		ll = sql.getTimestamps("select " + col + " from sqlutil_data where v_serial>? limit 100", bindVars3);
		test(ll != null && ll.length == 100, "get" + typ + "s test 3");
		Object[] bindVars4 = { null, null, new Integer(0) };
		int[] sqlTypes4 = { Types.DATE, Types.DATE, Types.INTEGER };
		ll = sql.getTimestamps(
				"select " + col + " from sqlutil_data where (? is null or " + col + "=?) and  v_serial>? limit 100",
				bindVars4, sqlTypes4);
		test(ll != null && ll.length == 100, "get" + typ + "s test 4");
	}

	private static void testLong() throws SQLException {
		System.out.println("test getLong* methods");
		String col = "v_numeric18";
		String typ = "Long";
		Long l = sql.getLong("select " + col + " from sqlutil_data where " + col + " is null limit 1");
		test(l == null, "get" + typ + " test 1");
		l = sql.getLong("select " + col + " from sqlutil_data where " + col + " is not null limit 1");
		test(l != null, "get" + typ + " test 2");

		l = sql.getLongVarArgs("select " + col + " from sqlutil_data where " + col + " is null limit ?",
				new Integer(1));
		test(l == null, "get" + typ + " v_integer 3");
		l = sql.getLongVarArgs("select " + col + " from sqlutil_data where " + col + " is not null limit ?",
				new Integer(1));
		test(l != null, "get" + typ + " test 4");

		Object[] bindVars = { new Integer(1) };
		l = sql.getLong("select " + col + " from sqlutil_data where " + col + " is null limit ?", bindVars);
		test(l == null, "get" + typ + " test 5");
		l = sql.getLong("select " + col + " from sqlutil_data where " + col + " is not null limit ?", bindVars);
		test(l != null, "get" + typ + " test 6");

		Object[] bindVars2a = { null, null, new Integer(1) };
		int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
		l = sql.getLong("select " + col + " from sqlutil_data where (? is null or ?=v_serial) limit ?", bindVars2a,
				sqlTypes2);
		test(l == null, "get" + typ + " test 7");
		Object[] bindVars2b = { new Integer(1), new Integer(2), new Integer(1) };
		l = sql.getLong("select " + col + " from sqlutil_data where (? is null or ?=v_serial) limit ?", bindVars2b,
				sqlTypes2);
		test(l != null, "get" + typ + " test 8");

		// getBigDecimals
		Long[] ll = sql.getLongs("select " + col + " from sqlutil_data where " + col + " is null limit 100");
		test(ll != null && ll.length == 100, "get" + typ + "s test 1");
		ll = sql.getLongsVarArgs("select " + col + " from sqlutil_data where v_serial>? limit 100", new Integer(0));
		test(ll != null && ll.length == 100, "get" + typ + "s test 2");
		Object[] bindVars3 = { new Integer(0) };
		ll = sql.getLongs("select " + col + " from sqlutil_data where v_serial>? limit 100", bindVars3);
		test(ll != null && ll.length == 100, "get" + typ + "s test 3");
		Object[] bindVars4 = { null, null, new Integer(0) };
		int[] sqlTypes4 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
		ll = sql.getLongs(
				"select " + col + " from sqlutil_data where (? is null or " + col + "=?) and  v_serial>? limit 100",
				bindVars4, sqlTypes4);
		test(ll != null && ll.length == 100, "get" + typ + "s test 4");
	}

	private static void testString() throws SQLException {
		System.out.println("test getString* methods");
		String col = "v_text";
		String typ = "String";
		String l = sql.getString("select " + col + " from sqlutil_data where " + col + " is null limit 1");
		test(l == null, "get" + typ + " test 1");
		l = sql.getString("select " + col + " from sqlutil_data where " + col + " is not null limit 1");
		test(l != null, "get" + typ + " test 2");

		l = sql.getStringVarArgs("select " + col + " from sqlutil_data where " + col + " is null limit ?",
				new Integer(1));
		test(l == null, "get" + typ + " v_integer 3");
		l = sql.getStringVarArgs("select " + col + " from sqlutil_data where " + col + " is not null limit ?",
				new Integer(1));
		test(l != null, "get" + typ + " test 4");

		Object[] bindVars = { new Integer(1) };
		l = sql.getString("select " + col + " from sqlutil_data where " + col + " is null limit ?", bindVars);
		test(l == null, "get" + typ + " test 5");
		l = sql.getString("select " + col + " from sqlutil_data where " + col + " is not null limit ?", bindVars);
		test(l != null, "get" + typ + " test 6");

		Object[] bindVars2a = { null, null, new Integer(1) };
		int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
		l = sql.getString("select " + col + " from sqlutil_data where (? is null or ?=v_serial) limit ?", bindVars2a,
				sqlTypes2);
		test(l == null, "get" + typ + " test 7");
		Object[] bindVars2b = { new Integer(1), new Integer(2), new Integer(1) };
		l = sql.getString("select " + col + " from sqlutil_data where (? is null or ?=v_serial) limit ?", bindVars2b,
				sqlTypes2);
		test(l != null, "get" + typ + " test 8");

		// getStrings
		String[] ll = sql.getStrings("select " + col + " from sqlutil_data where " + col + " is null limit 100");
		test(ll != null && ll.length == 100, "get" + typ + "s test 1");
		ll = sql.getStringsVarArgs("select " + col + " from sqlutil_data where v_serial>? limit 100", new Integer(0));
		test(ll != null && ll.length == 100, "get" + typ + "s test 2");
		Object[] bindVars3 = { new Integer(0) };
		ll = sql.getStrings("select " + col + " from sqlutil_data where v_serial>? limit 100", bindVars3);
		test(ll != null && ll.length == 100, "get" + typ + "s test 3");
		Object[] bindVars4 = { null, "bla", new Integer(0) };
		int[] sqlTypes4 = { Types.INTEGER, Types.VARCHAR, Types.INTEGER };
		ll = sql.getStrings(
				"select " + col + " from sqlutil_data where (? is null or " + col + "=?) and  v_serial>? limit 100",
				bindVars4, sqlTypes4);
		test(ll != null && ll.length == 100, "get" + typ + "s test 4");
	}

	private static void testBigDecimal() throws SQLException {
		System.out.println("test getBigDecimal* methods");
		String col = "v_numeric38";
		String typ = "BigDecimal";
		BigDecimal l = sql.getBigDecimal("select " + col + " from sqlutil_data where " + col + " is null limit 1");
		test(l == null, "get" + typ + " test 1");
		l = sql.getBigDecimal("select " + col + " from sqlutil_data where " + col + " is not null limit 1");
		test(l != null, "get" + typ + " test 2");

		l = sql.getBigDecimalVarArgs("select " + col + " from sqlutil_data where " + col + " is null limit ?",
				new Integer(1));
		test(l == null, "get" + typ + " v_integer 3");
		l = sql.getBigDecimalVarArgs("select " + col + " from sqlutil_data where " + col + " is not null limit ?",
				new Integer(1));
		test(l != null, "get" + typ + " test 4");

		Object[] bindVars = { new Integer(1) };
		l = sql.getBigDecimal("select " + col + " from sqlutil_data where " + col + " is null limit ?", bindVars);
		test(l == null, "get" + typ + " test 5");
		l = sql.getBigDecimal("select " + col + " from sqlutil_data where " + col + " is not null limit ?", bindVars);
		test(l != null, "get" + typ + " test 6");

		Object[] bindVars2a = { null, null, new Integer(1) };
		int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
		l = sql.getBigDecimal("select " + col + " from sqlutil_data where (? is null or ?=v_serial) limit ?",
				bindVars2a, sqlTypes2);
		test(l == null, "get" + typ + " test 7");
		Object[] bindVars2b = { new Integer(1), new Integer(2), new Integer(1) };
		l = sql.getBigDecimal("select " + col + " from sqlutil_data where (? is null or ?=v_serial) limit ?",
				bindVars2b, sqlTypes2);
		test(l != null, "get" + typ + " test 8");

		// getBigDecimals
		BigDecimal[] ll = sql
				.getBigDecimals("select " + col + " from sqlutil_data where " + col + " is null limit 100");
		test(ll != null && ll.length == 100, "get" + typ + "s test 1");
		ll = sql.getBigDecimalsVarArgs("select " + col + " from sqlutil_data where v_serial>? limit 100",
				new Integer(0));
		test(ll != null && ll.length == 100, "get" + typ + "s test 2");
		Object[] bindVars3 = { new Integer(0) };
		ll = sql.getBigDecimals("select " + col + " from sqlutil_data where v_serial>? limit 100", bindVars3);
		test(ll != null && ll.length == 100, "get" + typ + "s test 3");
		Object[] bindVars4 = { null, null, new Integer(0) };
		int[] sqlTypes4 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
		ll = sql.getBigDecimals(
				"select " + col + " from sqlutil_data where (? is null or " + col + "=?) and  v_serial>? limit 100",
				bindVars4, sqlTypes4);
		test(ll != null && ll.length == 100, "get" + typ + "s test 4");
	}

	private static void testRaw() throws SQLException {
		System.out.println("test getRaw* methods");
		String col = "v_bytea";
		String typ = "byte[]";
		byte[] l = sql.getRaw("select " + col + " from sqlutil_data where " + col + " is null limit 1");
		test(l == null, "get" + typ + " test 1");
		l = sql.getRaw("select " + col + " from sqlutil_data where " + col + " is not null limit 1");
		test(l != null, "get" + typ + " test 2");

		l = sql.getRawVarArgs("select " + col + " from sqlutil_data where " + col + " is null limit ?",
				new Integer(1));
		test(l == null, "get" + typ + " v_integer 3");
		l = sql.getRawVarArgs("select " + col + " from sqlutil_data where " + col + " is not null limit ?",
				new Integer(1));
		test(l != null, "get" + typ + " test 4");

		Object[] bindVars = { new Integer(1) };
		l = sql.getRaw("select " + col + " from sqlutil_data where " + col + " is null limit ?", bindVars);
		test(l == null, "get" + typ + " test 5");
		l = sql.getRaw("select " + col + " from sqlutil_data where " + col + " is not null limit ?", bindVars);
		test(l != null, "get" + typ + " test 6");

		Object[] bindVars2a = { null, null, new Integer(1) };
		int[] sqlTypes2 = { Types.INTEGER, Types.INTEGER, Types.INTEGER };
		l = sql.getRaw("select " + col + " from sqlutil_data where (? is null or ?=v_serial) limit ?",
				bindVars2a, sqlTypes2);
		test(l == null, "get" + typ + " test 7");
		Object[] bindVars2b = { new Integer(1), new Integer(2), new Integer(1) };
		l = sql.getRaw("select " + col + " from sqlutil_data where (? is null or ?=v_serial) limit ?",
				bindVars2b, sqlTypes2);
		test(l != null, "get" + typ + " test 8");

		// getRaws
		byte[][] ll = sql
				.getRaws("select " + col + " from sqlutil_data where " + col + " is null limit 100");
		test(ll != null && ll.length == 100, "get" + typ + "s test 1");
		ll = sql.getRawsVarArgs("select " + col + " from sqlutil_data where v_serial>? limit 100",
				new Integer(0));
		test(ll != null && ll.length == 100, "get" + typ + "s test 2");
		Object[] bindVars3 = { new Integer(0) };
		ll = sql.getRaws("select " + col + " from sqlutil_data where v_serial>? limit 100", bindVars3);
		test(ll != null && ll.length == 100, "get" + typ + "s test 3");
		Object[] bindVars4 = { null, null, new Integer(0) };
		int[] sqlTypes4 = { Types.VARBINARY, Types.VARBINARY, Types.INTEGER };
		ll = sql.getRaws(
				"select " + col + " from sqlutil_data where (? is null or " + col + "=?) and  v_serial>? limit 100",
				bindVars4, sqlTypes4);
		test(ll != null && ll.length == 100, "get" + typ + "s test 4");
	}


	private static void test(boolean b, String msg) {
		if (!b)
			throw new RuntimeException("Testcase failed; " + msg);
	}

	// create byte array, filled with 0xDEAD, length between 0 and 1000.
	private static byte[] bytea() {
		int len = rand.nextInt(1000);
		byte[] a = new byte[len];
		for (int i = 0; i < len; i++) {
			switch (i % 4) {
			case 0:
				a[i] = 0xD;
				break;
			case 1:
				a[i] = 0xE;
				break;
			case 2:
				a[i] = 0xA;
				break;
			case 3:
				a[i] = 0xD;
				break;
			}
		}
		return a;
	}

	// create a string, based on param i. 123 will return onetwothree, param len=max length of string.
	private static String weirdString(int i, int len) {
		StringBuffer sb = new StringBuffer(100);
		if (i == 0)
			return "zero";
		String[] nums = { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine" };
		while (i != 0) {
			int mod = i % 10;
			sb.append(nums[mod]);
			i = i / 10;
		}
		return len > sb.length() ? sb.toString() : sb.toString().substring(0, len);
	}

}
