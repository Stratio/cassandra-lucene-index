package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;

import java.util.Iterator;
import java.util.List;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ListBasedPartitionIterator implements PartitionIterator {

    private final Iterator<ListBasedRowIterator> iterator;

    public ListBasedPartitionIterator(List<ListBasedRowIterator> rowsIterators) {
        iterator = rowsIterators.iterator();
    }

    public void close() {

    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public RowIterator next() {
        return iterator.next();
    }
}
