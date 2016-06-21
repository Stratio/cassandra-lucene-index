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
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.utils.concurrent.OpOrder;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;

/**
 * {@link IndexWriter} for wide rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class IndexWriterWide extends IndexWriter {

    private final NavigableSet<Clustering> rowsToRead;
    private final Map<Clustering, Optional<Row>> rows;

    /**
     * Builds a new {@link IndexWriter} for tables with wide rows.
     *
     * @param service the service to perform the indexing operation
     * @param key key of the partition being modified
     * @param nowInSec current time of the update operation
     * @param opGroup operation group spanning the update operation
     * @param transactionType what kind of update is being performed on the base data
     */
    IndexWriterWide(IndexServiceWide service,
                    DecoratedKey key,
                    int nowInSec,
                    OpOrder.Group opGroup,
                    IndexTransaction.Type transactionType) {
        super(service, key, nowInSec, opGroup, transactionType);
        rowsToRead = service.clusterings();
        rows = new LinkedHashMap<>();
    }

    /** {@inheritDoc} */
    @Override
    protected void delete() {
        service.delete(key);
        rowsToRead.clear();
        rows.clear();
    }

    /** {@inheritDoc} */
    @Override
    protected void index(Row row) {
        if (!row.isStatic()) {
            Clustering clustering = row.clustering();
            if (service.needsReadBeforeWrite(key, row)) {
                Tracer.trace("Lucene index doing read before write");
                rowsToRead.add(clustering);
                rows.put(clustering, Optional.empty());
            } else {
                Tracer.trace("Lucene index skipping read before write");
                rows.put(clustering, Optional.of(row));
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void finish() {
        if (transactionType != IndexTransaction.Type.CLEANUP) {

            // Read required rows from storage engine
            service.read(key, rowsToRead, nowInSec, opGroup).forEachRemaining(unfiltered -> {
                Row row = (Row) unfiltered;
                rows.put(row.clustering(), Optional.of(row));
            });

            // Write rows
            rows.forEach((clustering, optional) -> optional.ifPresent(row -> {
                if (row.hasLiveData(nowInSec)) {
                    Tracer.trace("Lucene index writing document");
                    service.upsert(key, row, nowInSec);
                } else {
                    Tracer.trace("Lucene index deleting document");
                    service.delete(key, row);
                }
            }));
        }
    }
}
