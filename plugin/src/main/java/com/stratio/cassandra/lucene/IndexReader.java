/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.index.DocumentIterator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

/**
 * {@link UnfilteredPartitionIterator} for retrieving rows from a {@link DocumentIterator}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
abstract class IndexReader implements UnfilteredPartitionIterator {

    protected final ReadCommand command;
    protected final ColumnFamilyStore table;
    protected final ReadOrderGroup orderGroup;
    protected final DocumentIterator documents;
    protected UnfilteredRowIterator next;

    /**
     * Constructor taking the Cassandra read data and the Lucene results iterator.
     *
     * @param command the read command
     * @param table the base table
     * @param orderGroup the order group of the read operation
     * @param documents the documents iterator
     */
    IndexReader(ReadCommand command, ColumnFamilyStore table, ReadOrderGroup orderGroup, DocumentIterator documents) {
        this.command = command;
        this.table = table;
        this.orderGroup = orderGroup;
        this.documents = documents;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isForThrift() {
        return command.isForThrift();
    }

    /** {@inheritDoc} */
    @Override
    public CFMetaData metadata() {
        return table.metadata;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return prepareNext();
    }

    /** {@inheritDoc} */
    @Override
    public UnfilteredRowIterator next() {
        if (next == null) {
            prepareNext();
        }
        UnfilteredRowIterator result = next;
        next = null;
        return result;
    }

    protected abstract boolean prepareNext();

    /** {@inheritDoc} */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        try {
            if (next != null) {
                next.close();
            }
        } finally {
            documents.close();
        }
    }

    protected UnfilteredRowIterator read(DecoratedKey key, ClusteringIndexFilter filter) {
        return SinglePartitionReadCommand.create(isForThrift(),
                                                 table.metadata,
                                                 command.nowInSec(),
                                                 command.columnFilter(),
                                                 command.rowFilter(),
                                                 command.limits(),
                                                 key,
                                                 filter).queryMemtableAndDisk(table, orderGroup.baseReadOpOrderGroup());
    }
}
