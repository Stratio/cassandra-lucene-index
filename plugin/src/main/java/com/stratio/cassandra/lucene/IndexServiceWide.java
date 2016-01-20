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
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
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
import org.apache.lucene.search.*;

import java.util.NavigableSet;
import java.util.Optional;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

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

    /** {@inheritDoc} */
    @Override
    public Query query(DataRange dataRange) {
        PartitionPosition startPosition = dataRange.startKey();
        PartitionPosition stopPosition = dataRange.stopKey();
        Token startToken = startPosition.getToken();
        Token stopToken = stopPosition.getToken();
        boolean isSameToken = startToken.compareTo(stopToken) == 0 && !tokenMapper.isMinimum(startToken);
        BooleanClause.Occur occur = isSameToken ? FILTER : SHOULD;
        boolean includeStart = tokenMapper.includeStart(startPosition);
        boolean includeStop = tokenMapper.includeStop(stopPosition);
        logger.debug("INCLUDE START {}", includeStart);
        logger.debug("INCLUDE STOP {}", includeStop);

        ClusteringPrefix startBound = null;
        if (startPosition instanceof DecoratedKey) {
            DecoratedKey key = (DecoratedKey) startPosition;
            ClusteringIndexSliceFilter cisf = (ClusteringIndexSliceFilter) dataRange.clusteringIndexFilter(key);
            logger.debug("CLUSTERING INDEX FILTER {}", cisf);
            Slices slices = cisf.requestedSlices();
            logger.debug("SLICES {}", slices);
            startBound = slices.get(0).start();
            logger.debug("START BOUND {}", startBound);
        }

        ClusteringPrefix stopBound = null;
        if (stopPosition instanceof DecoratedKey) {
            DecoratedKey key = (DecoratedKey) stopPosition;
            ClusteringIndexSliceFilter cisf = (ClusteringIndexSliceFilter) dataRange.clusteringIndexFilter(key);
            logger.debug("CLUSTERING INDEX FILTER {}", cisf);
            Slices slices = cisf.requestedSlices();
            logger.debug("SLICES {}", slices);
            stopBound = slices.get(slices.size() - 1).end();
            logger.debug("STOP BOUND {}", stopBound);
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        if (startBound != null) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            b.add(tokenMapper.query(startToken), FILTER);
            b.add(clusteringMapper.filter(startBound, null), FILTER);
            builder.add(b.build(), occur);
            includeStart = false;
        }

        if (stopBound != null) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            b.add(tokenMapper.query(stopToken), FILTER);
            b.add(clusteringMapper.filter(null, stopBound), FILTER);
            builder.add(b.build(), occur);
            includeStop = false;
        }

        BooleanQuery query = builder.build();
        if (!isSameToken) {
            Query rangeFilter = tokenMapper.query(startToken, stopToken, includeStart, includeStop);
            if (rangeFilter != null) {
                builder.add(rangeFilter, SHOULD);
                query = builder.build();
            }
        } else if (query.clauses().isEmpty()) {
            return tokenMapper.query(startToken);
        }

        return query.clauses().isEmpty() ? null : new QueryWrapperFilter(query);
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

    /** {@inheritDoc} */
    @Override
    public UnfilteredPartitionIterator read(Query query,
                                            Sort sort,
                                            ScoreDoc after,
                                            ReadCommand command,
                                            ReadExecutionController executionController) {
        return new IndexPartitionIterator(command, table, lucene, query, sort, after, fieldsToLoad) {

            private Document nextDoc;

            @Override
            protected boolean prepareNext() {

                if (next != null) {
                    return true;
                }

                if (nextDoc == null) {
                    if (!documents.hasNext()) {
                        return false;
                    }
                    nextDoc = documents.next();
                }

                NavigableSet<Clustering> clusterings = clusterings();
                DecoratedKey key = partitionMapper.decoratedKey(nextDoc);

                while (nextDoc != null && key.getKey().equals(partitionMapper.decoratedKey(nextDoc).getKey())) {
                    Clustering clustering = clusteringMapper.clustering(nextDoc);
                    if (command.selectsKey(key) && command.selectsClustering(key, clustering)) {
                        clusterings.add(clustering);
                    }
                    nextDoc = documents.hasNext() ? documents.next() : null;
                }

                if (clusterings.isEmpty()) {
                    return prepareNext();
                }

                ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
                SinglePartitionReadCommand dataCommand = SinglePartitionReadCommand.create(table.metadata,
                                                                                           command.nowInSec(),
                                                                                           command.columnFilter(),
                                                                                           command.rowFilter(),
                                                                                           DataLimits.NONE,
                                                                                           key,
                                                                                           filter);
                UnfilteredRowIterator data = dataCommand.queryMemtableAndDisk(table, executionController);

                if (data.isEmpty()) {
                    data.close();
                    return prepareNext();
                }

                next = data;
                return true;
            }
        };
    }

}
