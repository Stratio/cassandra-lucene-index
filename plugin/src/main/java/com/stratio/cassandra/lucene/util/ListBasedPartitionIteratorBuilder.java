package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ListBasedPartitionIteratorBuilder {

    CFMetaData metadata;
    PartitionColumns columns;
    List<ListBasedRowIterator> iterators;

    public ListBasedPartitionIteratorBuilder(CFMetaData metadata,
                                             PartitionColumns columns) {
        this.metadata = metadata;
        this.columns = columns;
        iterators = new ArrayList<>();
    }

    public void add(DecoratedKey key, Row staticRow, List<Row> rows) {
        iterators.add(new ListBasedRowIterator(metadata, key, columns, staticRow, rows));
    }

    public void add(DecoratedKey key, Row staticRow, Row row) {
        iterators.add(new ListBasedRowIterator(metadata, key, columns, staticRow, Collections.singletonList(row)));
    }

    public ListBasedPartitionIterator build() {
        return new ListBasedPartitionIterator(iterators);
    }
}
