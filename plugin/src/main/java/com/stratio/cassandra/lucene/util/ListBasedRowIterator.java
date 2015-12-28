package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ListBasedRowIterator implements RowIterator {

    CFMetaData metadata;
    DecoratedKey key;
    PartitionColumns columns;
    Row staticRow;
    Iterator<Row> rows;

    public ListBasedRowIterator(CFMetaData metadata,
                                DecoratedKey key,
                                PartitionColumns columns,
                                Row staticRow,
                                List<Row> rows) {
        this.metadata = metadata;
        this.key = key;
        this.columns = columns;
        this.staticRow = staticRow;
        this.rows = rows.iterator();
    }

    /**
     * The metadata for the table this iterator on.
     */
    public CFMetaData metadata() {
        return metadata;
    }

    /**
     * Whether or not the rows returned by this iterator are in reversed
     * clustering order.
     */
    public boolean isReverseOrder() {
        return false;
    }

    /**
     * A subset of the columns for the (static and regular) rows returned by this iterator.
     * Every row returned by this iterator must guarantee that it has only those columns.
     */
    public PartitionColumns columns() {
        return columns;
    }

    /**
     * The partition key of the partition this in an iterator over.
     */
    public DecoratedKey partitionKey() {
        return key;
    }

    /**
     * The static part corresponding to this partition (this can be an empty
     * row).
     */
    public Row staticRow() {
        return staticRow;
    }

    public void close() {

    }

    public boolean hasNext() {
        return rows.hasNext();
    }

    public Row next() {
        return rows.next();
    }
}
