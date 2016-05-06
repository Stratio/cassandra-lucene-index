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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Index.Indexer} for Lucene-based index.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
abstract class IndexWriter implements Index.Indexer {

    protected static final Logger logger = LoggerFactory.getLogger(IndexWriter.class);

    protected final IndexService service;
    protected final DecoratedKey key;
    protected final int nowInSec;
    protected final OpOrder.Group opGroup;
    protected final IndexTransaction.Type transactionType;

    /**
     * Abstract constructor.
     *
     * @param service the service to perform the indexing operation
     * @param key key of the partition being modified
     * @param nowInSec current time of the update operation
     * @param opGroup operation group spanning the update operation
     * @param transactionType what kind of update is being performed on the base data
     */
    IndexWriter(IndexService service,
                DecoratedKey key,
                int nowInSec,
                OpOrder.Group opGroup,
                IndexTransaction.Type transactionType) {
        this.service = service;
        this.key = key;
        this.nowInSec = nowInSec;
        this.opGroup = opGroup;
        this.transactionType = transactionType;
    }

    /** {@inheritDoc} */
    @Override
    public void begin() {
    }

    /** {@inheritDoc} */
    @Override
    public void partitionDelete(DeletionTime deletionTime) {
        logger.trace("Delete partition during {}: {}", transactionType, deletionTime);
        delete();
    }

    /** {@inheritDoc} */
    @Override
    public void rangeTombstone(RangeTombstone tombstone) {
        logger.trace("Range tombstone during {}: {}", transactionType, tombstone);
    }

    /** {@inheritDoc} */
    @Override
    public void insertRow(Row row) {
        logger.trace("Insert rows during {}: {}", transactionType, row);
        index(row);
    }

    /** {@inheritDoc} */
    @Override
    public void updateRow(Row oldRowData, Row newRowData) {
        logger.trace("Update row during {}: {} TO {}", transactionType, oldRowData, newRowData);
        index(newRowData);
    }

    /** {@inheritDoc} */
    @Override
    public void removeRow(Row row) {
        logger.trace("Remove row during {}: {}", transactionType, row);
        index(row);
    }

    /**
     * Deletes all the partition.
     */
    protected abstract void delete();

    /**
     * Indexes the specified partition's {@link Row}. It behaviours as an upsert and may involve read-before-write.
     *
     * @param row the row to be indexed.
     */
    protected abstract void index(Row row);
}
