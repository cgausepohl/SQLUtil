package com.binrock.sqlutil;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Hashtable;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.binrock.sqlutil.impl.ConversionHelper;

public class Row {
	
	private Object[] data;
	private Hashtable<String,Integer> columnIndices;
	
	public Row(Object[] data, final Hashtable<String,Integer> columnIndices) {
		this.data = data;
		this.columnIndices = columnIndices;
	}
	
	public Object[] getData() {
		return data;
	}
	
	public Object get(int idx) {
		return data[idx];
	}

	public Object get(String column) {
		return get(columnIndices.get(column));
	}
	
	public Boolean getBoolean(int idx) {
		return ConversionHelper.toBoolean(data[idx]);
	}
	
	public Boolean getBoolean(String column) {
		return getBoolean(columnIndices.get(column));
	}

	public Long getLong(int idx) {
		return ConversionHelper.toLong(data[idx]);
	}

	public Long getLong(String column) {
		return getLong(columnIndices.get(column));
	}
	
	public Timestamp getTimestamp(int idx) {
		return ConversionHelper.toTimestamp(data[idx]);
	}

	public Timestamp getTimestamp(String column) {
		return getTimestamp(columnIndices.get(column));
	}
	
	public BigDecimal getBigDecimal(int idx) {
		return ConversionHelper.toBigDecimal(data[idx]);
	}

	public BigDecimal getBigDecimal(String column) {
		return getBigDecimal(columnIndices.get(column));
	}
	
	public Double getDouble(int idx) {
		return ConversionHelper.toDouble(data[idx]);
	}

	public Double geDouble(String column) {
		return getDouble(columnIndices.get(column));
	}
	
	public String getString (int idx) {
		return ConversionHelper.toStr(data[idx]);
	}
	
	public String getString (String column) {
		return getString(columnIndices.get(column));
	}
	
	//public Hashtable<String, String> getI18nStrings(int idx) {
	//	Hashtable<String, String> i18ns = new Hashtable<>();
	//	String s = getString(idx);
	//	try {
	//		JsonReader reader = Json.createReader(new StringReader(s));
	//		JsonObject jsonObject = reader.readObject();
	//		for (String langCode:jsonObject.keySet()) {
	//			String val = jsonObject.getString(langCode);
	//			i18ns.put(langCode, val);
	//		}
	//	} catch (JsonParsingException jpe) {
	//		throw new JsonParsingException("string was: "+s, jpe, jpe.getLocation());
	//	}
	//	return i18ns;
	//}

	//public Hashtable<String, String> getI18nStrings(String column) {
	//	return getI18nStrings(columnIndices.get(column));
	//}

	public String getRaw (int idx) {
		return ConversionHelper.toRaw(data[idx]);
	}
	
	public String getRaw (String column) {
		return getString(columnIndices.get(column));
	}

	public Integer getInt(int idx) {
		return ConversionHelper.toInteger(data[idx]);
	}

	public Integer getInt(String column) {
		return getInt(columnIndices.get(column));
	}
	
	public JsonObject getJson(int idx) {
		String s = getString(idx);
		if (s==null) return null;
		JsonReader r = Json.createReader(new StringReader(s));
		JsonObject jo = r.readObject();
		r.close();
		return jo;
	}
	
	public JsonObject getJson(String column) {
		return getJson(columnIndices.get(column));
	}

}
