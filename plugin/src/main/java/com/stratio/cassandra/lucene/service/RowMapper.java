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
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Class for several {@link Row} mappings between Cassandra and Lucene data models.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class RowMapper {

    /** The indexed table metadata. */
    final CFMetaData metadata;

    /** The indexed column definition. */
    final ColumnDefinition columnDefinition;

    /** The indexing schema. */
    final Schema schema;

    /** A token mapper for the indexed table. */
    final TokenMapper tokenMapper;

    /** A partition key mapper for the indexed table. */
    final PartitionKeyMapper partitionKeyMapper;

    /** A regular cell mapper for the indexed table. */
    final RegularCellsMapper regularCellsMapper;

    /**
     * Builds a new {@link RowMapper} for the specified {@link IndexConfig}.
     *
     * @param config The {@link IndexConfig}.
     */
    RowMapper(IndexConfig config) {
        this.metadata = config.getMetadata();
        this.columnDefinition = config.getColumnDefinition();
        this.schema = config.getSchema();
        this.tokenMapper = TokenMapper.instance();
        this.partitionKeyMapper = PartitionKeyMapper.instance(metadata, schema);
        this.regularCellsMapper = RegularCellsMapper.instance(metadata, schema);
    }

    /**
     * Returns a new {@link RowMapper} for the specified {@link IndexConfig}.
     *
     * @param config The {@link IndexConfig}.
     * @return A new {@link RowMapper} for the specified {@link IndexConfig}.
     */
    public static RowMapper build(IndexConfig config) {
        return config.isWide() ? new RowMapperWide(config) : new RowMapperSkinny(config);
    }

    /**
     * Returns the {@link Columns} representing the specified logic {@link Row}.
     *
     * @param partitionKey A logic {@link Row} partition key.
     * @param columnFamily A logic {@link Row} {@link ColumnFamily}.
     * @return The columns contained in the specified columns.
     */
    public abstract Columns columns(DecoratedKey partitionKey, ColumnFamily columnFamily);

    /**
     * Returns the {@link Columns} representing the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return The columns contained in the specified columns.
     */
    public final Columns columns(Row row) {
        return columns(row.key, row.cf);
    }

    /**
     * Returns the decorated partition key representing the specified raw partition key.
     *
     * @param key A partition key.
     * @return The decorated partition key representing the specified raw partition key.
     */
    public final DecoratedKey partitionKey(ByteBuffer key) {
        return partitionKeyMapper.partitionKey(key);
    }

    /**
     * Returns a Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key.
     *
     * @param partitionKey A decorated partition key.
     * @return A Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key.
     */
    public Term term(DecoratedKey partitionKey) {
        return partitionKeyMapper.term(partitionKey);
    }

    /**
     * Returns a Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link DataRange}.
     *
     * @param dataRange A {@link DataRange}.
     * @return A Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link DataRange}.
     */
    public abstract Query query(DataRange dataRange);

    /**
     * Returns a Lucene {@link Query} to get the {@link Document} with the specified {@link RowKey}.
     *
     * @param rowKey A {@link RowKey}.
     * @return A Lucene {@link Query} to get the {@link Document} with the specified {@link RowKey}.
     */
    public abstract Query query(RowKey rowKey);

    /**
     * Returns the Lucene {@link SortField}s to get {@link Document}s in the same order that is used in Cassandra.
     *
     * @return The Lucene {@link SortField}s to get {@link Document}s in the same order that is used in Cassandra.
     */
    public abstract List<SortField> sortFields();

    /**
     * Returns a {@link CellName} for the indexed column in the specified column family.
     *
     * @param columnFamily A column family.
     * @return A {@link CellName} for the indexed column in the specified column family.
     */
    public abstract CellName makeCellName(ColumnFamily columnFamily);

    /**
     * Returns a {@link Row} {@link Comparator} using the same order that is used in Cassandra.
     *
     * @return A {@link Row} {@link Comparator} using the same order that is used in Cassandra.
     */
    protected abstract Comparator<Row> comparator();

    /**
     * Returns the {@link Comparator} to be used for ordering the {@link Row}s obtained from the specified {@link
     * Search}. This {@link Comparator} is useful for merging the partial results obtained from running the specified
     * {@link Search} against several indexes.
     *
     * @param search A {@link Search}.
     * @return The {@link Comparator} to be used for ordering the {@link Row}s obtained from the specified {@link
     * Search}.
     */
    public Comparator<Row> comparator(Search search) {
        List<Comparator<Row>> comparators = new ArrayList<>();
        if (search.usesSorting()) {
            final Comparator<Columns> comparator = search.getSort().comparator();
            comparators.add(new Comparator<Row>() {
                @Override
                public int compare(Row row1, Row row2) {
                    Columns columns1 = columns(row1);
                    Columns columns2 = columns(row2);
                    return comparator.compare(columns1, columns2);
                }
            });
        }
        if (search.usesRelevance()) {
            comparators.add(new Comparator<Row>() {
                @Override
                public int compare(Row row1, Row row2) {
                    Float score1 = score(row1);
                    Float score2 = score(row2);
                    return score2.compareTo(score1);
                }
            });
        }
        comparators.add(comparator());
        return Ordering.compound(comparators);
    }

    /**
     * Returns the {@link SearchResult} defined by the specified {@link Document} and {@link ScoreDoc}.
     *
     * @param document A {@link Document}.
     * @param scoreDoc A {@link ScoreDoc}.
     * @return The {@link SearchResult} defined by the specified {@link Document} and {@link ScoreDoc}.
     */
    public abstract SearchResult searchResult(Document document, ScoreDoc scoreDoc);

    /**
     * Returns the score of the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return The score of the specified {@link Row}.
     */
    protected Float score(Row row) {
        ColumnFamily cf = row.cf;
        CellName cellName = makeCellName(cf);
        Cell cell = cf.getColumn(cellName);
        String value = UTF8Type.instance.compose(cell.value());
        return Float.parseFloat(value);
    }

    /**
     * Returns the {@link RowKey} represented by the specified {@link ByteBuffer}.
     *
     * @param bb A {@link ByteBuffer}.
     * @return The {@link RowKey} represented by the specified {@link ByteBuffer}.
     */
    public abstract RowKey rowKey(ByteBuffer bb);

    /**
     * Returns a {@link ByteBuffer} representing the specified {@link RowKey}.
     *
     * @param rowKey A {@link RowKey}.
     * @return A {@link ByteBuffer} representing the specified {@link RowKey}.
     */
    public abstract ByteBuffer byteBuffer(RowKey rowKey);

    /**
     * Returns a {@link ByteBuffer} representing the specified {@link RowKeys}.
     *
     * @param rowKeys A {@link RowKeys}.
     * @return A {@link ByteBuffer} representing the specified {@link RowKeys}.
     */
    public ByteBuffer byteBuffer(RowKeys rowKeys) {

        List<byte[]> allBytes = new ArrayList<>(rowKeys.size());
        int size = 0;
        for (RowKey rowKey : rowKeys) {
            byte[] bytes = ByteBufferUtils.asArray(byteBuffer(rowKey));
            allBytes.add(bytes);
            size += bytes.length + 4;
        }

        ByteBuffer bb = ByteBuffer.allocate(size);
        for (byte[] bytes : allBytes) {
            bb.putInt(bytes.length);
            bb.put(bytes);
        }
        bb.rewind();
        return bb;
    }

    /**
     * Returns the {@link RowKeys} represented by the specified {@link ByteBuffer}.
     *
     * @param bb A {@link ByteBuffer}.
     * @return The {@link RowKeys} represented by the specified {@link ByteBuffer}.
     */
    public RowKeys rowKeys(ByteBuffer bb) {
        RowKeys rowKeys = new RowKeys();
        bb.rewind();
        while (bb.hasRemaining()) {
            int size = bb.getInt();
            byte[] bytes = new byte[size];
            bb.get(bytes);
            RowKey rowKey = rowKey(ByteBuffer.wrap(bytes));
            rowKeys.add(rowKey);
        }
        bb.rewind();
        return rowKeys;
    }

    /**
     * Returns a {@link RowKey} representing the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return A{@link RowKey} representing the specified {@link Row}.
     */
    public abstract RowKey rowKey(Row row);

}
