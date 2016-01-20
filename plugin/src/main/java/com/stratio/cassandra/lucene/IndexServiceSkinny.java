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

import com.stratio.cassandra.lucene.column.Columns;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;

import java.util.NavigableSet;
import java.util.Optional;

/**
 * Class for providing operations between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexServiceSkinny extends IndexService {

    /**
     * Constructor using the specified {@link IndexOptions}.
     *
     * @param table The indexed table.
     * @param indexMetadata The index metadata.
     */
    protected IndexServiceSkinny(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        super(table, indexMetadata);
    }

    /** {@inheritDoc} */
    @Override
    public IndexWriterSkinny indexWriter(DecoratedKey key,
                                         int nowInSec,
                                         OpOrder.Group opGroup,
                                         IndexTransaction.Type transactionType) {
        return new IndexWriterSkinny(this, key, nowInSec, opGroup, transactionType);
    }

    /** {@inheritDoc} */
    @Override
    public Columns columns(DecoratedKey key, Row row) {
        Columns columns = new Columns();
        partitionMapper.addColumns(columns, key);
        columnsMapper.addColumns(columns, row);
        return columns;
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Document> document(DecoratedKey key, Row row) {
        Document document = new Document();
        Columns columns = columns(key, row);
        schema.addFields(document, columns);
        if (document.getFields().isEmpty()) {
            return Optional.empty();
        } else {
            tokenMapper.addFields(document, key);
            partitionMapper.addFields(document, key);
            return Optional.of(document);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Term term(DecoratedKey key, Row row) {
        return partitionMapper.term(key);
    }

    /** {@inheritDoc} */
    @Override
    public Term term(Document document) {
        return partitionMapper.term(document);
    }

    /** {@inheritDoc} */
    @Override
    public Query query(DataRange dataRange) {
        PartitionPosition startPosition = dataRange.startKey();
        PartitionPosition stopPosition = dataRange.stopKey();
        Token startToken = startPosition.getToken();
        Token stopToken = stopPosition.getToken();
        boolean includeStart = tokenMapper.includeStart(startPosition);
        boolean includeStop = tokenMapper.includeStop(stopPosition);
        return tokenMapper.query(startToken, stopToken, includeStart, includeStop);
    }

    public Optional<Row> read(DecoratedKey key, int nowInSec, OpOrder.Group opGroup) {
        NavigableSet<Clustering> clusterings = clusterings(Clustering.EMPTY);
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
        ColumnFilter columnFilter = ColumnFilter.all(table.metadata);
        UnfilteredRowIterator iterator;
        iterator = SinglePartitionReadCommand.create(table.metadata,
                                                     nowInSec,
                                                     key,
                                                     columnFilter,
                                                     filter)
                                             .queryMemtableAndDisk(table, opGroup);
        return iterator.hasNext() ? Optional.of((Row) iterator.next()) : Optional.empty();
    }

    /** {@inheritDoc} */
    @Override
    public UnfilteredPartitionIterator read(Query query,
                                            Sort sort,
                                            ScoreDoc after,
                                            ReadCommand command,
                                            ReadExecutionController executionController) {
        return new IndexPartitionIterator(command, table, lucene, query, sort, after, fieldsToLoad) {
            @Override
            protected boolean prepareNext() {
                while (next == null && documents.hasNext()) {
                    DecoratedKey key = partitionMapper.decoratedKey(documents.next());
                    SinglePartitionReadCommand dataCommand;
                    dataCommand = SinglePartitionReadCommand.create(isForThrift(),
                                                                    table.metadata,
                                                                    command.nowInSec(),
                                                                    command.columnFilter(),
                                                                    command.rowFilter(),
                                                                    DataLimits.NONE,
                                                                    key,
                                                                    command.clusteringIndexFilter(key));
                    UnfilteredRowIterator data = dataCommand.queryMemtableAndDisk(table, executionController);

                    if (data != null) {
                        if (data.isEmpty()) {
                            data.close();
                        } else {
                            next = data;
                        }
                    }
                }
                return next != null;
            }
        };

    }
}
