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

package com.stratio.cassandra.lucene.service;

import com.google.common.collect.Ordering;
import com.stratio.cassandra.lucene.IndexConfig;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * {@link RowMapper} for wide rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RowMapperWide extends RowMapper {

    /** The clustering key mapper. */
    private final ClusteringKeyMapper clusteringKeyMapper;

    /** The full key mapper. */
    private final FullKeyMapper fullKeyMapper;

    /**
     * Builds a new {@link RowMapperWide} for the specified {@link IndexConfig}.
     *
     * @param config The {@link IndexConfig}.
     */
    RowMapperWide(IndexConfig config) {
        super(config);
        this.clusteringKeyMapper = ClusteringKeyMapper.instance(metadata, schema);
        this.fullKeyMapper = FullKeyMapper.instance(partitionKeyMapper, clusteringKeyMapper);
    }

    /** {@inheritDoc} */
    @Override
    public Columns columns(DecoratedKey partitionKey, ColumnFamily columnFamily) {
        Columns columns = new Columns();
        columns.add(partitionKeyMapper.columns(partitionKey));
        columns.add(clusteringKeyMapper.columns(columnFamily));
        columns.add(regularCellsMapper.columns(columnFamily));
        return columns;
    }

    /**
     * Returns a Lucene {@link Document} representing the logical CQL row represented by the specified partition key,
     * clustering key and {@link Columns}.
     *
     * @param partitionKey  The partition key of the logical CQL row.
     * @param clusteringKey The clustering key of the logical CQL row.
     * @param columns       The {@link Columns} of the logical CQL row.
     * @return A Lucene {@link Document} representing the specified logical CQL row
     */
    public Document document(DecoratedKey partitionKey, CellName clusteringKey, Columns columns) {
        Document document = new Document();
        tokenMapper.addFields(document, partitionKey);
        partitionKeyMapper.addFields(document, partitionKey);
        clusteringKeyMapper.addFields(document, clusteringKey);
        fullKeyMapper.addFields(document, partitionKey, clusteringKey);
        schema.addFields(document, columns);
        return document;
    }

    /** {@inheritDoc} */
    @Override
    public List<SortField> sortFields() {
        List<SortField> sortFields = new ArrayList<>();
        sortFields.addAll(tokenMapper.sortFields());
        sortFields.addAll(clusteringKeyMapper.sortFields());
        return sortFields;
    }

    /** {@inheritDoc} */
    @Override
    public CellName makeCellName(ColumnFamily columnFamily) {
        CellName clusteringKey = clusteringKey(columnFamily);
        return clusteringKeyMapper.makeCellName(clusteringKey, columnDefinition);
    }

    /** {@inheritDoc} */
    @Override
    public Comparator<Row> comparator() {
        return Ordering.compound(Arrays.asList(tokenMapper.comparator(), clusteringKeyMapper.comparator()));
    }

    /**
     * Returns the first clustering key contained in the specified {@link ColumnFamily}.
     *
     * @param columnFamily A {@link ColumnFamily}.
     * @return The first clustering key contained in the specified {@link ColumnFamily}.
     */
    private CellName clusteringKey(ColumnFamily columnFamily) {
        return clusteringKeyMapper.clusteringKey(columnFamily);
    }

    /**
     * Returns the Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key and
     * clustering key.
     *
     * @param partitionKey  A decorated partition key.
     * @param clusteringKey A clustering key.
     * @return The Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key and
     * clustering key.
     */
    public Term term(DecoratedKey partitionKey, CellName clusteringKey) {
        return fullKeyMapper.term(partitionKey, clusteringKey);
    }

    /** {@inheritDoc} */
    @Override
    public Query query(DataRange dataRange) {

        RowPosition startPosition = dataRange.startKey();
        RowPosition stopPosition = dataRange.stopKey();
        Token startToken = startPosition.getToken();
        Token stopToken = stopPosition.getToken();
        boolean isSameToken = startToken.compareTo(stopToken) == 0 && !tokenMapper.isMinimum(startToken);
        BooleanClause.Occur occur = isSameToken ? FILTER : SHOULD;
        boolean includeStart = tokenMapper.includeStart(startPosition);
        boolean includeStop = tokenMapper.includeStop(stopPosition);

        SliceQueryFilter sqf;
        if (startPosition instanceof DecoratedKey) {
            sqf = (SliceQueryFilter) dataRange.columnFilter(((DecoratedKey) startPosition).getKey());
        } else {
            sqf = (SliceQueryFilter) dataRange.columnFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        }
        Composite startName = sqf.start();
        Composite stopName = sqf.finish();

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        if (!startName.isEmpty()) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            b.add(tokenMapper.query(startToken), FILTER);
            b.add(clusteringKeyMapper.query(startName, null), FILTER);
            builder.add(b.build(), occur);
            includeStart = false;
        }

        if (!stopName.isEmpty()) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            b.add(tokenMapper.query(stopToken), FILTER);
            b.add(clusteringKeyMapper.query(null, stopName), FILTER);
            builder.add(b.build(), occur);
            includeStop = false;
        }

        BooleanQuery query = builder.build();
        if (!isSameToken) {
            Query rangeQuery = tokenMapper.query(startToken, stopToken, includeStart, includeStop);
            if (rangeQuery != null) {
                builder.add(rangeQuery, SHOULD);
                query = builder.build();
            }
        } else if (query.clauses().isEmpty()) {
            return tokenMapper.query(startToken);
        }

        return query.clauses().isEmpty() ? null : query;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(RowKey rowKey) {
        DecoratedKey partitionKey = rowKey.getPartitionKey();
        CellName clusteringKey = rowKey.getClusteringKey();
        Term term = term(partitionKey, clusteringKey);
        return new TermQuery(term);
    }

    /**
     * Returns the Lucene {@link Query} to get the {@link Document}s satisfying the specified partition key and {@link
     * RangeTombstone}.
     *
     * @param partitionKey   A partition key.
     * @param rangeTombstone A {@link RangeTombstone}.
     * @return The Lucene {@link Query} to get the {@link Document}s satisfying the specified partition key and {@link
     * RangeTombstone}.
     */
    public Query query(DecoratedKey partitionKey, RangeTombstone rangeTombstone) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(partitionKeyMapper.query(partitionKey), FILTER);
        builder.add(clusteringKeyMapper.query(rangeTombstone.min, rangeTombstone.max), FILTER);
        return builder.build();
    }

    /**
     * Returns the array of {@link ColumnSlice}s for selecting the logic CQL3 row identified by the specified clustering
     * keys.
     *
     * @param clusteringKeys A list of clustering keys.
     * @return The array of {@link ColumnSlice}s for selecting the logic CQL3 row identified by the specified clustering
     * keys.
     */
    public ColumnSlice[] columnSlices(List<CellName> clusteringKeys) {
        return clusteringKeyMapper.columnSlices(clusteringKeys);
    }

    /**
     * Returns the logical CQL3 column families contained in the specified physical {@link ColumnFamily}.
     *
     * @param columnFamily A physical {@link ColumnFamily}.
     * @return The logical CQL3 column families contained in the specified physical {@link ColumnFamily}.
     */
    public Map<CellName, ColumnFamily> splitRows(ColumnFamily columnFamily) {
        return clusteringKeyMapper.splitRows(columnFamily);
    }

    /** {@inheritDoc} */
    @Override
    public SearchResult searchResult(Document document, ScoreDoc scoreDoc) {
        DecoratedKey partitionKey = partitionKeyMapper.partitionKey(document);
        CellName clusteringKey = clusteringKeyMapper.clusteringKey(document);
        return new SearchResult(partitionKey, clusteringKey, scoreDoc);
    }

    /**
     * Returns a hash code to uniquely identify a CQL logical row key.
     *
     * @param partitionKey  A partition key.
     * @param clusteringKey A clustering key.
     * @return A hash code to uniquely identify a CQL logical row key.
     */
    public String hash(DecoratedKey partitionKey, CellName clusteringKey) {
        return fullKeyMapper.hash(partitionKey, clusteringKey);
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer byteBuffer(RowKey rowKey) {
        DecoratedKey partitionKey = rowKey.getPartitionKey();
        CellName clusteringKey = rowKey.getClusteringKey();
        return fullKeyMapper.byteBuffer(partitionKey, clusteringKey);
    }

    /** {@inheritDoc} */
    @Override
    public RowKey rowKey(ByteBuffer bb) {
        return fullKeyMapper.rowKey(bb);
    }

    /** {@inheritDoc} */
    @Override
    public RowKey rowKey(Row row) {
        DecoratedKey partitionKey = row.key;
        CellName clusteringKey = clusteringKey(row.cf);
        return new RowKey(partitionKey, clusteringKey);
    }
}
