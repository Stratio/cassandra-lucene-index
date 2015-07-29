package com.stratio.cassandra.lucene.service;

import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A sorted list of {@link RowKey}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RowKeys implements Iterable<RowKey> {

    /** The {@link RowKey}s. */
    private final List<RowKey> rowKeys;

    /**
     * Default empty constructor.
     */
    public RowKeys() {
        this(new ArrayList<RowKey>());
    }

    /**
     * Constructor taking a initial list of {@link RowKey}s.
     * @param rowKeys A list of {@link RowKey}s.
     */
    public RowKeys(List<RowKey> rowKeys) {
        this.rowKeys = rowKeys;
    }

    /** {@inheritDoc} */
    public Iterator<RowKey> iterator() {
        return rowKeys.iterator();
    }

    /**
     * Returns the number of {@link RowKey}s in this list.
     *
     * @return the number of {@link RowKey}s in this list
     */
    public int size() {
        return rowKeys.size();
    }

    /**
     * Adds a new {@link RowKey} to this list.
     * @param rowKey A {@link RowKey} to be added.
     */
    public void add(RowKey rowKey) {
        rowKeys.add(rowKey);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("rowKeys", rowKeys).toString();
    }
}
