/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class IndexWriter implements org.apache.cassandra.index.Index.Indexer {

    protected static final Logger logger = LoggerFactory.getLogger(IndexWriter.class);

    protected final DecoratedKey key;
    protected final int nowInSec;
    protected final OpOrder.Group opGroup;
    protected final IndexTransaction.Type transactionType;

    /**
     * Abstract constructor.
     *
     * @param key key of the partition being modified
     * @param nowInSec current time of the update operation
     * @param opGroup operation group spanning the update operation
     * @param transactionType indicates what kind of update is being performed on the base data i.e. a write time
     * insert/update/delete or the result of compaction
     */
    protected IndexWriter(DecoratedKey key,
                          int nowInSec,
                          OpOrder.Group opGroup,
                          IndexTransaction.Type transactionType) {
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
        logger.debug("Delete partition during {}: {}", transactionType, deletionTime);
        delete();
    }

    /** {@inheritDoc} */
    @Override
    public void rangeTombstone(RangeTombstone tombstone) {
        logger.debug("Range tombstone during {}: {}", transactionType, tombstone);
    }

    /** {@inheritDoc} */
    @Override
    public void insertRow(Row row) {
        logger.debug("Insert rows during {}: {}", transactionType, row);
        index(row);
    }

    /** {@inheritDoc} */
    @Override
    public void updateRow(Row oldRowData, Row newRowData) {
        logger.debug("Update row during {}: {} TO {}", transactionType, oldRowData, newRowData);
        index(newRowData);
    }

    /** {@inheritDoc} */
    @Override
    public void removeRow(Row row) {
        logger.debug("Remove row during {}: {}", transactionType, row);
        index(row);
    }

    protected abstract void delete();

    protected abstract void index(Row row);
}
