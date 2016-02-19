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
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.utils.concurrent.OpOrder;

import java.util.Optional;

/**
 * {@link IndexWriter} for skinny rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexWriterSkinny extends IndexWriter {

    private Optional<Row> row;

    /**
     * Builds a new {@link IndexWriter} for tables with skinny rows.
     *
     * @param service the service to perform the indexing operation
     * @param key key of the partition being modified
     * @param nowInSec current time of the update operation
     * @param opGroup operation group spanning the update operation
     * @param transactionType what kind of update is being performed on the base data
     */
    public IndexWriterSkinny(IndexServiceSkinny service,
                             DecoratedKey key,
                             int nowInSec,
                             OpOrder.Group opGroup,
                             IndexTransaction.Type transactionType) {
        super(service, key, nowInSec, opGroup, transactionType);
        row = Optional.empty();
    }

    /** {@inheritDoc} */
    @Override
    protected void delete() {
        service.delete(key);
        row = Optional.empty();
    }

    /** {@inheritDoc} */
    @Override
    protected void index(Row row) {
        this.row = Optional.of(row);
    }

    /** {@inheritDoc} */
    @Override
    public void finish() {
        row.ifPresent(row -> {
            if (service.needsReadBeforeWrite(key, row)) {
                UnfilteredRowIterator iterator = service.read(key, nowInSec, opGroup);
                if (iterator.hasNext()) {
                    row = (Row) iterator.next();
                }
            }
            if (row.hasLiveData(nowInSec)) {
                service.upsert(key, row);
            } else {
                service.delete(key);
            }
        });
    }
}
