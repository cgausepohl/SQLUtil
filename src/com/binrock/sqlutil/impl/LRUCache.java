package com.binrock.sqlutil.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

public final class LRUCache {

    // TODO: Softreferences?
    // TODO: maybe remove this thing completly. it seems to be the case, that there is no difference
    //       in caching preparedstmts or not. I think the driver is caching them somehow internally.
    //       so, maybe this class and the SQLUtil-PS-Caching can be removed completly.

    private LinkedList<String> lruSQL = new LinkedList<>();
    private Hashtable<String, PreparedStatement> stmts = new Hashtable<>();
    private int maxSize = 0;

    public LRUCache() {}

    public LRUCache(int maxSize) {
        this.maxSize = maxSize;
    }

    public int size() {
        return lruSQL.size();
    }

    public boolean contains(String sql) {
        return lruSQL.contains(sql);
    }

    public PreparedStatement get(String sql) {
        boolean removed = lruSQL.remove(sql);
        if (!removed)
            return null;
        lruSQL.addFirst(sql);
        return stmts.get(sql);
    }

    private void removeDeadStatements() throws SQLException {
        List<String> del = new ArrayList<>();
        for (String sql: lruSQL) {
            PreparedStatement ps = stmts.get(sql);
            if (ps==null)
                del.add(sql);
            else
                if (ps.isClosed())
                    del.add(sql);
        }
        for (String sql: del)
            remove(sql);
    }

    public void add(String sql, PreparedStatement s) throws SQLException {
        removeDeadStatements();
        if (maxSize>0)
            while (size()>=maxSize)
                removeLast();
        lruSQL.addFirst(sql);
        stmts.put(sql, s);
    }

    public boolean removeLast() throws SQLException {
        if (lruSQL.size()==0) return false;
        String last = lruSQL.getLast();
        return remove(last);
    }

    public boolean remove(String sql) throws SQLException {
        PreparedStatement stmt = stmts.get(sql);
        if (stmt!=null) {
            if (!stmt.isClosed())
                stmt.close();
            stmts.remove(sql);
        }
        return lruSQL.remove(sql);
    }

    public void resize(int newMaxSize) throws SQLException {
        this.maxSize = newMaxSize;
        while (size()>maxSize)
            removeLast();
    }

    public void clear() throws SQLException {
        while (lruSQL.size()>0)
            removeLast();
        assert(lruSQL.size()==0);
        assert(stmts.size()==0);
    }

}
