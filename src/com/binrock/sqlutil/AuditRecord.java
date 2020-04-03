package com.binrock.sqlutil;

import java.sql.Timestamp;

public class AuditRecord {
    private long t0ns, t1ns, tStartms;
    private Integer rowcount;
    private Throwable error;
    private Object[] bindValues;
    private String sql;

    public AuditRecord(String sql) {
        t0ns = System.nanoTime();
        tStartms = System.currentTimeMillis();
        this.sql = sql;
    }

    public AuditRecord(String sql, Object[] stmtParams) {
        this(sql);
        bindValues = stmtParams;
    }

    public String getSQL() {
        return sql;
    }

    public Integer getRowsAffected() {
        return rowcount;
    }

    public Timestamp getStarted() {
        return new Timestamp(tStartms);
    }

    public Float getDurationMs() {
        if (error!=null) return null;
        Float f = (t1ns-t0ns)/(float)1000000;
        return f;
    }

    public Throwable getError() {
        return error;
    }

    public String getBindValues() {
        if (bindValues==null) return null;
        StringBuffer sb = new StringBuffer();
        boolean first = true;
        for (Object o: bindValues) {
            if (!first) sb.append(',');
            first=false;
            if (o==null)
                sb.append("null");
            else
                sb.append(o.getClass().getName()).append('[').append(o.toString()).append(']');
        }
        return sb.toString();
    }

    public void finish() {
        t1ns = System.nanoTime();
    }

    public void finish(int rowcount) {
        finish();
        // only use rowcount if no error was recorded before
        if (error == null)
            this.rowcount = rowcount;
    }

    public void finish(Throwable t) {
        finish();
        error = t;
    }

    @Override
    public String toString() {
        String sBind;
        if (bindValues != null) {
            StringBuffer sb = new StringBuffer();
            boolean first = true;
            for (Object v : bindValues) {
                if (!first)
                    sb.append(";");
                else
                    first = false;
                if (v != null)
                    sb.append(v.getClass().getName()).append('[').append(v.toString()).append(']');
                else
                    sb.append("null");
            }
            sBind = sb.toString();
        } else {
            sBind = "";
        }
        String sd = null;
        long diff = t1ns - t0ns;
        if (diff < 1000)
            sd = diff + "ns";
        else if (diff < 1000000)
            sd = (diff / 1000) + "µs";
        else if (diff < 1000000000)
            sd = (diff / 1000000) + "ms";
        else
            sd = (diff / 1000000000) + "s";
        int l = sd.length();
        if (l == 3)
            sd = "  " + sd;
        else if (l == 4)
            sd = " " + sd;
        String rows;
        if (rowcount == null)
            rows = "";
        else
            rows = rowcount + "rows;";
        String sStart = "[" + sd + ';' + rows + "start=" + tStartms + ";params={" + sBind + "}";
        if (error != null)
            return sStart + ";error=" + error.getMessage() + "]";

        if (t0ns > 0 && t1ns > 0)
            return sStart + ";OK]";

        throw new IllegalStateException("t0ns=" + t0ns + " t1ns=" + t0ns + " tStartms=" + tStartms
                + " error=" + error + " bindValues=" + sBind);
    }
}
