package com.binrock.sqlutil;

@Deprecated
public class Timing {
/*
    private String name, sqlStmt;
    private long tStart, tEnd=-1;
    private Integer msDuration;

    public Timing(String name) {
        this.name = name;
        tStart = System.currentTimeMillis();
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Timing) {
            Timing t2 = (Timing)obj;
            if (!name.equals(t2.getName())) return false;
            if (tStart!=t2.getStarted()) return false;
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return Timing.class.getName()+"{\""+name+"\":"+msDuration+"ms, started="+(new Date(tStart))+"; sql=["+sqlStmt+"]}";
    }

    public long getStarted() {
        return tStart;
    }

    public String getName() {
        return name;
    }

    public void setSQLStmt(String sqlStmt) {
        this.sqlStmt = sqlStmt;
    }

    public void endTiming() {
        tEnd = System.currentTimeMillis();
        msDuration = (int)(tEnd-tStart);
    }

    public Integer getDuration() {
        return msDuration;
    }

    public String getSQLStmt() {
        return sqlStmt;
    }
    */
}
