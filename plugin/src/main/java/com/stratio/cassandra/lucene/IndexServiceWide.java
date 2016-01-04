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
import com.stratio.cassandra.lucene.key.ClusteringMapper;
import com.stratio.cassandra.lucene.key.KeyMapper;
import com.stratio.cassandra.lucene.key.PartitionMapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;

import java.util.*;

/**
 * Class for providing operations between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexServiceWide extends IndexService {

    private final ClusteringMapper clusteringMapper;
    private final KeyMapper keyMapper;

    /**
     * Constructor using the specified {@link IndexOptions}.
     *
     * @param table The indexed table.
     * @param indexMetadata The index metadata.
     */
    protected IndexServiceWide(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        super(table, indexMetadata);
        clusteringMapper = new ClusteringMapper(table.metadata);
        keyMapper = new KeyMapper(partitionMapper, clusteringMapper);
        fieldsToLoad.add(ClusteringMapper.FIELD_NAME);
        fieldsToLoad.add(KeyMapper.FIELD_NAME);
        keySortFields.add(clusteringMapper.sortField());
    }

    /** {@inheritDoc} */
    @Override
    public IndexWriterWide indexWriter(DecoratedKey key,
                                       int nowInSec,
                                       OpOrder.Group opGroup,
                                       IndexTransaction.Type transactionType) {
        return new IndexWriterWide(this, key, nowInSec, opGroup, transactionType);
    }

    /** {@inheritDoc} */
    @Override
    public Columns columns(DecoratedKey key, Row row) {
        Clustering clustering = row.clustering();
        Columns columns = new Columns();
        partitionMapper.addColumns(columns, key);
        clusteringMapper.addColumns(columns, clustering);
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
            Clustering clustering = row.clustering();
            tokenMapper.addFields(document, key);
            partitionMapper.addFields(document, key);
            clusteringMapper.addFields(document, clustering);
            keyMapper.addFields(document, key, clustering);
            return Optional.of(document);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Term term(DecoratedKey key, Row row) {
        return term(key, row.clustering());
    }

    private Term term(DecoratedKey key, Clustering clustering) {
        return keyMapper.term(key, clustering);
    }

    /** {@inheritDoc} */
    @Override
    public Term term(Document document) {
        return keyMapper.term(document);
    }

    /**
     * Retrieves from the local storage the {@link Row}s of the specified partition slice.
     *
     * @param key the partition key
     * @param clusterings the clustering keys
     * @param now max allowed time in seconds
     * @param group operation group spanning the calling operation
     * @return a partition {@link Row} iterator
     */
    public UnfilteredRowIterator rows(DecoratedKey key,
                                      NavigableSet<Clustering> clusterings,
                                      int now,
                                      OpOrder.Group group) {
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
        ColumnFilter columnFilter = ColumnFilter.all(table.metadata);
        return SinglePartitionReadCommand.create(table.metadata, now, key, columnFilter, filter)
                                         .queryMemtableAndDisk(table, group);
    }

    public UnfilteredPartitionIterator read(Query query, Sort sort, ScoreDoc after, ReadCommand command,ReadExecutionController executionController) {
        int count = command.limits().count();
        logger.debug("COUNT {}", count);
        IndexSearcher searcher = lucene.acquireSearcher();
        Iterator<Document> documents = lucene.search(searcher, query, sort, after, count, fieldsToLoad);
        return new UnfilteredPartitionIterator() {
            private Document nextEntry;

            private UnfilteredRowIterator next;

            public boolean isForThrift() {
                return command.isForThrift();
            }

            public CFMetaData metadata() {
                return command.metadata();
            }

            public boolean hasNext() {
                return prepareNext();
            }

            public UnfilteredRowIterator next() {
                if (next == null)
                    prepareNext();

                UnfilteredRowIterator toReturn = next;
                next = null;
                return toReturn;
            }

            private boolean prepareNext() {
                if (next != null)
                    return true;

                if (nextEntry == null) {
                    if (!documents.hasNext())
                        return false;

                    nextEntry = documents.next();
                }

                NavigableSet<Clustering> clusterings = clusterings();
                DecoratedKey partitionKey = partitionMapper.decoratedKey(nextEntry);

                while (nextEntry != null && partitionKey.getKey().equals(partitionMapper.decoratedKey(nextEntry).getKey())) {
                    // We're queried a slice of the index, but some hits may not match some of the clustering column constraints
                    if (isMatchingEntry(partitionKey, nextEntry, command)) {
                        clusterings.add(clusteringMapper.clustering(nextEntry));
                    }

                    nextEntry = documents.hasNext() ? documents.next() : null;
                }

                // Because we've eliminated entries that don't match the clustering columns, it's possible we added nothing
                if (clusterings.isEmpty())
                    return prepareNext();

                // Query the gathered index hits. We still need to filter stale hits from the resulting query.
                ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
                SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(table.metadata,
                                                                                       command.nowInSec(),
                                                                                       command.columnFilter(),
                                                                                       command.rowFilter(),
                                                                                       DataLimits.NONE,
                                                                                       partitionKey,
                                                                                       filter);
                UnfilteredRowIterator dataIter = dataCmd.queryMemtableAndDisk(table, executionController);


                if (dataIter.isEmpty()) {
                    dataIter.close();
                    return prepareNext();
                }

                next = dataIter;
                return true;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }

            public void close() {
                lucene.releaseSearcher(searcher);
                if (next != null)
                    next.close();
            }
        };
    }

    private boolean isMatchingEntry(DecoratedKey partitionKey, Document entry, ReadCommand command)
    {
        return command.selectsKey(partitionKey) && command.selectsClustering(partitionKey, clusteringMapper.clustering(entry));
    }
}
