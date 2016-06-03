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
class IndexServiceWide extends IndexService {

    private final KeyMapper keyMapper;

    /**
     * Constructor using the specified {@link IndexOptions}.
     *
     * @param table the indexed table
     * @param indexMetadata the index metadata
     */
    IndexServiceWide(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        super(table, indexMetadata);
        keyMapper = new KeyMapper(metadata);
        super.init();
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
    protected void addKeyFields(Document document, DecoratedKey key, Row row) {
        Clustering clustering = row.clustering();
        tokenMapper.addFields(document, key);
        partitionMapper.addFields(document, key);
        keyMapper.addFields(document, key, clustering);
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
        return filter.selectsAllPartition() ? partitionMapper.query(key) : keyMapper.query(key, filter);
    }

    private Query query(PartitionPosition position) {
        return position instanceof DecoratedKey
               ? partitionMapper.query((DecoratedKey) position)
               : tokenMapper.query(position.getToken());
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Query> query(DataRange dataRange) {

        // Check trivia
        if (dataRange.isUnrestricted()) {
            return Optional.empty();
        }

        // Extract data range data
        PartitionPosition startPosition = dataRange.startKey();
        PartitionPosition stopPosition = dataRange.stopKey();
        Token startToken = startPosition.getToken();
        Token stopToken = stopPosition.getToken();
        ClusteringPrefix startClustering = KeyMapper.startClusteringPrefix(dataRange).orElse(null);
        ClusteringPrefix stopClustering = KeyMapper.stopClusteringPrefix(dataRange).orElse(null);
        boolean includeStartClustering = startClustering != null && startClustering.size() > 0;
        boolean includeStopClustering = stopClustering != null && stopClustering.size() > 0;

        // Try single partition
        if (startToken.compareTo(stopToken) == 0) {
            if (!includeStartClustering && !includeStopClustering) {
                return Optional.of(query(startPosition));
            }
            return Optional.of(keyMapper.query(startPosition, startClustering, stopClustering));
        }

        // Prepare query builder
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        // Add first partition filter
        if (includeStartClustering) {
            builder.add(keyMapper.query(startPosition, startClustering, null), SHOULD);
        }

        // Add token range filter
        boolean includeStartToken = startPosition.kind() == MIN_BOUND && !includeStartClustering;
        boolean includeStopToken = stopPosition.kind() == MAX_BOUND && !includeStopClustering;
        tokenMapper.query(startToken, stopToken, includeStartToken, includeStopToken)
                   .ifPresent(x -> builder.add(x, SHOULD));

        // Add last partition filter
        if (includeStopClustering) {
            builder.add(keyMapper.query(stopPosition, null, stopClustering), SHOULD);
        }

        // Return query, or empty if there are no restrictions
        BooleanQuery query = builder.build();
        return query.clauses().isEmpty() ? Optional.empty() : Optional.of(query);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Query> after(DecoratedKey key, Clustering clustering) {
        if (key == null) {
            return Optional.empty();
        } else if (clustering == null) {
            return Optional.of(partitionMapper.query(key));
        } else {
            return Optional.of(keyMapper.query(key, clustering));
        }
    }

    /** {@inheritDoc} */
    @Override
    public IndexReaderWide indexReader(DocumentIterator documents, ReadCommand command, ReadOrderGroup orderGroup) {
        return new IndexReaderWide(this, command, table, orderGroup, documents);
    }

}
