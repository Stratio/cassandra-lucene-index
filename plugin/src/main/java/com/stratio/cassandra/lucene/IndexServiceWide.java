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

import com.stratio.cassandra.lucene.cache.SearchCacheUpdater;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.index.DocumentIterator;
import com.stratio.cassandra.lucene.key.KeyMapper;
import com.stratio.cassandra.lucene.key.PartitionMapper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

import java.util.*;

import static org.apache.cassandra.db.PartitionPosition.Kind.MAX_BOUND;
import static org.apache.cassandra.db.PartitionPosition.Kind.MIN_BOUND;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * {@link IndexService} for wide rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexServiceWide extends IndexService {

    private final KeyMapper keyMapper;

    /**
     * Constructor using the specified {@link IndexOptions}.
     *
     * @param table the indexed table
     * @param indexMetadata the index metadata
     */
    protected IndexServiceWide(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        super(table, indexMetadata);
        keyMapper = new KeyMapper(metadata);
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> fieldsToLoad() {
        return new HashSet<>(Arrays.asList(PartitionMapper.FIELD_NAME, KeyMapper.FIELD_NAME));
    }

    /** {@inheritDoc} */
    @Override
    public List<SortField> keySortFields() {
        return Arrays.asList(tokenMapper.sortField(), keyMapper.sortField());
    }

    /**
     * Returns the clustering key contained in the specified {@link Document}.
     *
     * @param document a {@link Document} containing the clustering key to be get
     * @return the clustering key contained in {@code document}
     */
    public Clustering clustering(Document document) {
        return keyMapper.clustering(document);
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
        keyMapper.addColumns(columns, clustering);
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
            keyMapper.addFields(document, key, clustering);
            return Optional.of(document);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Term term(DecoratedKey key, Row row) {
        return term(key, row.clustering());
    }

    /**
     * Returns a Lucene {@link Term} identifying the {@link Document} representing the {@link Row} identified by the
     * specified partition and clustering keys.
     *
     * @param key the partition key
     * @param clustering the clustering key
     * @return the term identifying the document
     */
    private Term term(DecoratedKey key, Clustering clustering) {
        return keyMapper.term(key, clustering);
    }

    /** {@inheritDoc} */
    @Override
    public Term term(Document document) {
        return KeyMapper.term(document);
    }

    /** {@inheritDoc} */
    @Override
    public Query query(DecoratedKey key, ClusteringIndexFilter filter) {
        return keyMapper.query(key, filter);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Query> query(DataRange dataRange) {

        // Extract data range data
        PartitionPosition startPosition = dataRange.startKey();
        PartitionPosition stopPosition = dataRange.stopKey();
        Token startToken = startPosition.getToken();
        Token stopToken = stopPosition.getToken();
        Optional<ClusteringPrefix> maybeStartClustering = KeyMapper.startClusteringPrefix(dataRange);
        Optional<ClusteringPrefix> maybeStopClustering = KeyMapper.stopClusteringPrefix(dataRange);

        // Prepare query builder
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        // Add first partition filter
        maybeStartClustering.ifPresent(startClustering -> {
            DecoratedKey startKey = (DecoratedKey) startPosition;
            builder.add(keyMapper.query(startKey, startClustering, null, false, true), SHOULD);
        });

        // Add token range filter
        boolean includeStart = startPosition.kind() == MIN_BOUND && !maybeStartClustering.isPresent();
        boolean includeStop = stopPosition.kind() == MAX_BOUND && !maybeStopClustering.isPresent();
        tokenMapper.query(startToken, stopToken, includeStart, includeStop).ifPresent(x -> builder.add(x, SHOULD));

        // Add last partition filter
        maybeStopClustering.ifPresent(stopClustering -> {
            DecoratedKey stopKey = (DecoratedKey) stopPosition;
            builder.add(keyMapper.query(stopKey, null, stopClustering, true, false), SHOULD);
        });

        // Return query, or empty if there are no restrictions
        BooleanQuery query = builder.build();
        return query.clauses().isEmpty() ? Optional.empty() : Optional.of(query);
    }

    /** {@inheritDoc} */
    @Override
    public IndexReaderWide indexReader(DocumentIterator documents,
                                       ReadCommand command,
                                       ReadOrderGroup orderGroup,
                                       SearchCacheUpdater cacheUpdater) {
        return new IndexReaderWide(this, command, table, orderGroup, documents, cacheUpdater);
    }

}
