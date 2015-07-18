package com.stratio.cassandra.lucene;

import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowKeys implements Iterable<RowKey>{

    private final List<RowKey> rowKeys;

    public RowKeys() {
        this(new ArrayList<RowKey>());
    }

    public RowKeys(List<RowKey> rowKeys) {
        this.rowKeys = rowKeys;
    }

    public List<RowKey> getRowKeys() {
        return rowKeys;
    }

    public Iterator<RowKey> iterator() {
        return rowKeys.iterator();
    }

    public int size() {
        return rowKeys.size();
    }

    public void add(RowKey rowKey) {
        rowKeys.add(rowKey);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("rowKeys", rowKeys).toString();
    }
}
