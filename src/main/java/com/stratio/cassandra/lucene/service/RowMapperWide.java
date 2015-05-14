/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.service;

import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import java.util.List;
import java.util.Map;

import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * {@link RowMapper} for wide rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowMapperWide extends RowMapper {

    /** The clustering key mapper. */
    private final ClusteringKeyMapper clusteringKeyMapper;

    /** The full key mapper. */
    private final FullKeyMapper fullKeyMapper;

    /**
     * Builds a new {@link RowMapperWide} for the specified column family metadata, indexed column definition and {@link
     * Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link Schema}.
     */
    RowMapperWide(CFMetaData metadata, ColumnDefinition columnDefinition, Schema schema) {
        super(metadata, columnDefinition, schema);
        this.clusteringKeyMapper = ClusteringKeyMapper.instance(metadata);
        this.fullKeyMapper = FullKeyMapper.instance(partitionKeyMapper, clusteringKeyMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Columns columns(Row row) {
        Columns columns = new Columns();
        columns.add(partitionKeyMapper.columns(row));
        columns.add(clusteringKeyMapper.columns(row));
        columns.add(regularCellsMapper.columns(row));
        return columns;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document document(Row row) {
        DecoratedKey partitionKey = row.key;
        CellName clusteringKey = clusteringKeyMapper.clusteringKey(row);

        Document document = new Document();
        tokenMapper.addFields(document, partitionKey);
        partitionKeyMapper.addFields(document, partitionKey);
        clusteringKeyMapper.addFields(document, clusteringKey);
        fullKeyMapper.addFields(document, partitionKey, clusteringKey);
        schema.addFields(document, columns(row));
        return document;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sort sort() {
        SortField[] partitionKeySort = tokenMapper.sortFields();
        SortField[] clusteringKeySort = clusteringKeyMapper.sortFields();
        return new Sort(ArrayUtils.addAll(partitionKeySort, clusteringKeySort));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CellName makeCellName(ColumnFamily columnFamily) {
        CellName clusteringKey = clusteringKey(columnFamily);
        return clusteringKeyMapper.makeCellName(clusteringKey, columnDefinition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowComparator naturalComparator() {
        return new RowComparatorNatural(clusteringKeyMapper);
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
     * Returns all the clustering keys contained in the specified {@link ColumnFamily}.
     *
     * @param columnFamily A {@link ColumnFamily}.
     * @return All the clustering keys contained in the specified {@link ColumnFamily}.
     */
    public List<CellName> clusteringKeys(ColumnFamily columnFamily) {
        return clusteringKeyMapper.clusteringKeys(columnFamily);
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

    /**
     * Returns the Lucene {@link Filter} to get the {@link Document}s satisfying the specified {@link DataRange}.
     *
     * @param dataRange A {@link DataRange}.
     * @return The Lucene {@link Filter} to get the {@link Document}s satisfying the specified {@link DataRange}.
     */
    public Query query(DataRange dataRange) {
        RowPosition startPosition = dataRange.startKey();
        RowPosition stopPosition = dataRange.stopKey();
        Token startToken = startPosition.getToken();
        Token stopToken = stopPosition.getToken();
        boolean isSameToken = startToken.compareTo(stopToken) == 0 && !tokenMapper.isMinimum(startToken);
        BooleanClause.Occur occur = isSameToken ? MUST : SHOULD;
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

        BooleanQuery query = new BooleanQuery();

        if (!startName.isEmpty()) {
            BooleanQuery q = new BooleanQuery();
            q.add(tokenMapper.query(startToken), MUST);
            q.add(clusteringKeyMapper.query(startName, null), MUST);
            query.add(q, occur);
            includeStart = false;
        }

        if (!stopName.isEmpty()) {
            BooleanQuery q = new BooleanQuery();
            q.add(tokenMapper.query(stopToken), MUST);
            q.add(clusteringKeyMapper.query(null, stopName), MUST);
            query.add(q, occur);
            includeStop = false;
        }

        if (!isSameToken) {
            Query rangeQuery = tokenMapper.query(startToken, stopToken, includeStart, includeStop);
            if (rangeQuery != null) query.add(rangeQuery, SHOULD);
        } else if (query.getClauses().length == 0) {
            return tokenMapper.query(startToken);
        }

        return query.getClauses().length == 0 ? null : query;
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
        BooleanQuery query = new BooleanQuery();
        query.add(partitionKeyMapper.query(partitionKey), MUST);
        query.add(clusteringKeyMapper.query(rangeTombstone.min, rangeTombstone.max), MUST);
        return query;
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

    /**
     * Returns the {@code String} human-readable representation of the specified cell name.
     *
     * @param cellName A cell name.
     * @return The {@code String} human-readable representation of the specified cell name.
     */
    public String toString(CellName cellName) {
        return clusteringKeyMapper.toString(cellName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SearchResult searchResult(Document document, ScoreDoc scoreDoc) {
        DecoratedKey partitionKey = partitionKeyMapper.partitionKey(document);
        CellName clusteringKey = clusteringKeyMapper.clusteringKey(document);
        return new SearchResult(partitionKey, clusteringKey, scoreDoc);
    }
}
