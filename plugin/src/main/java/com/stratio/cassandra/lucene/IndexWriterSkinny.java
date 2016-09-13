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

import com.stratio.cassandra.lucene.util.Tracer;
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
class IndexWriterSkinny extends IndexWriter {

    private Optional<Row> optionalRow;

    /**
     * Builds a new {@link IndexWriter} for tables with skinny rows.
     *
     * @param service the service to perform the indexing operation
     * @param key key of the partition being modified
     * @param nowInSec current time of the update operation
     * @param opGroup operation group spanning the update operation
     * @param transactionType what kind of update is being performed on the base data
     */
    IndexWriterSkinny(IndexServiceSkinny service,
                      DecoratedKey key,
                      int nowInSec,
                      OpOrder.Group opGroup,
                      IndexTransaction.Type transactionType) {
        super(service, key, nowInSec, opGroup, transactionType);
        optionalRow = Optional.empty();
    }

    /** {@inheritDoc} */
    @Override
    protected void delete() {
        service.delete(key);
        optionalRow = Optional.empty();
    }

    /** {@inheritDoc} */
    @Override
    protected void index(Row row) {
        optionalRow = Optional.of(row);
    }

    /** {@inheritDoc} */
    @Override
    public void finish() {
        if (transactionType != IndexTransaction.Type.CLEANUP) {
            optionalRow.ifPresent(row -> {
                if (transactionType == IndexTransaction.Type.COMPACTION || service.needsReadBeforeWrite(key, row)) {
                    Tracer.trace("Lucene index reading before write");
                    UnfilteredRowIterator iterator = service.read(key, nowInSec, opGroup);
                    if (iterator.hasNext()) {
                        row = (Row) iterator.next();
                    }
                }
                if (row.hasLiveData(nowInSec)) {
                    Tracer.trace("Lucene index writing document");
                    service.upsert(key, row, nowInSec);
                } else {
                    Tracer.trace("Lucene index deleting document");
                    service.delete(key);
                }
            });
        }
    }
}
