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
import com.stratio.cassandra.lucene.index.DocumentIterator;
import com.stratio.cassandra.lucene.key.ClusteringMapper;
import com.stratio.cassandra.lucene.key.KeyMapper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;

import java.util.Optional;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * {@link IndexService} for wide rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexServiceWide extends IndexService {

    private final ClusteringMapper clusteringMapper;
    private final KeyMapper keyMapper;

    /**
     * Constructor using the specified {@link IndexOptions}.
     *
     * @param table the indexed table
     * @param indexMetadata the index metadata
     */
    protected IndexServiceWide(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        super(table, indexMetadata);
        clusteringMapper = new ClusteringMapper(table.metadata);
        keyMapper = new KeyMapper(partitionMapper, clusteringMapper);
        fieldsToLoad.add(ClusteringMapper.FIELD_NAME);
        fieldsToLoad.add(KeyMapper.FIELD_NAME);
        keySortFields.add(clusteringMapper.sortField());
    }

    /**
     * Returns the clustering key contained in the specified {@link Document}.
     *
     * @param document a {@link Document} containing the clustering key to be get
     * @return the clustering key contained in {@code document}
     */
    public Clustering clustering(Document document) {
        return clusteringMapper.clustering(document);
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

        ClusteringPrefix startBound = null;
        if (startPosition instanceof DecoratedKey) {
            DecoratedKey key = (DecoratedKey) startPosition;
            ClusteringIndexSliceFilter cisf = (ClusteringIndexSliceFilter) dataRange.clusteringIndexFilter(key);
            Slices slices = cisf.requestedSlices();
            startBound = slices.get(0).start();
        }

        ClusteringPrefix stopBound = null;
        if (stopPosition instanceof DecoratedKey) {
            DecoratedKey key = (DecoratedKey) stopPosition;
            ClusteringIndexSliceFilter cisf = (ClusteringIndexSliceFilter) dataRange.clusteringIndexFilter(key);
            Slices slices = cisf.requestedSlices();
            stopBound = slices.get(slices.size() - 1).end();
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        if (startBound != null) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            b.add(tokenMapper.query(startToken), FILTER);
            b.add(clusteringMapper.query(startBound, null), FILTER);
            builder.add(b.build(), occur);
            includeStart = false;
        }

        if (stopBound != null) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            b.add(tokenMapper.query(stopToken), FILTER);
            b.add(clusteringMapper.query(null, stopBound), FILTER);
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

    /** {@inheritDoc} */
    @Override
    public IndexReaderWide indexReader(DocumentIterator documents,
                                       ReadCommand command,
                                       ReadExecutionController controller) {
        return new IndexReaderWide(command, table, controller, documents, this);
    }

}
