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
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CellName;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.nio.ByteBuffer;

/**
 * Class for several {@link Row} mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class RowMapper {

    protected final CFMetaData metadata; // The indexed table metadata
    protected final ColumnDefinition columnDefinition; // The indexed column definition
    protected final Schema schema; // The indexing schema

    protected final TokenMapper tokenMapper; // A token mapper for the indexed table
    protected final PartitionKeyMapper partitionKeyMapper; // A partition key mapper for the indexed table
    protected final RegularCellsMapper regularCellsMapper; // A regular cell mapper for the indexed table

    /**
     * Builds a new {@link RowMapper} for the specified column family metadata, indexed column definition and {@link
     * Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link Schema}.
     */
    RowMapper(CFMetaData metadata, ColumnDefinition columnDefinition, Schema schema) {
        this.metadata = metadata;
        this.columnDefinition = columnDefinition;
        this.schema = schema;
        this.tokenMapper = TokenMapper.instance(metadata);
        this.partitionKeyMapper = PartitionKeyMapper.instance(metadata);
        this.regularCellsMapper = RegularCellsMapper.instance(metadata);
    }

    /**
     * Returns a new {@link RowMapper} for the specified column family metadata, indexed column definition and {@link
     * Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link Schema}.
     * @return A new {@link RowMapper} for the specified column family metadata, indexed column definition and {@link
     * Schema}.
     */
    public static RowMapper build(CFMetaData metadata, ColumnDefinition columnDefinition, Schema schema) {
        if (metadata.clusteringColumns().size() > 0) {
            return new RowMapperWide(metadata, columnDefinition, schema);
        } else {
            return new RowMapperSkinny(metadata, columnDefinition, schema);
        }
    }

    /**
     * Returns the {@link Columns} representing the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return The columns contained in the specified columns.
     */
    public abstract Columns columns(Row row);

    /**
     * Returns the {@link Document} representing the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return The {@link Document} representing the specified {@link Row}.
     */
    public abstract Document document(Row row);

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
     * Returns the decorated partition key contained in the specified {@link Document}.
     *
     * @param document A {@link Document}.
     * @return The decorated partition key contained in the specified {@link Document}.
     */
    public final DecoratedKey partitionKey(Document document) {
        return partitionKeyMapper.partitionKey(document);
    }

    /**
     * Returns the Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key.
     *
     * @param partitionKey A decorated partition key.
     * @return The Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key.
     */
    public Term term(DecoratedKey partitionKey) {
        return partitionKeyMapper.term(partitionKey);
    }

    /**
     * Returns the Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link DataRange}.
     *
     * @param dataRange A {@link DataRange}.
     * @return The Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link DataRange}.
     */
    public abstract Query query(DataRange dataRange);

    /**
     * Returns a {@link CellName} for the indexed column in the specified column family.
     *
     * @param columnFamily A column family.
     * @return A {@link CellName} for the indexed column in the specified column family.
     */
    public abstract CellName makeCellName(ColumnFamily columnFamily);

    /**
     * Returns a {@link RowComparator} using the same order that is used in Cassandra.
     *
     * @return A {@link RowComparator} using the same order that is used in Cassandra.
     */
    public abstract RowComparator naturalComparator();

    /**
     * Returns the {@link SearchResult} defined by the specified {@link Document} and {@link ScoreDoc}.
     *
     * @param document A {@link Document}.
     * @param scoreDoc A {@link ScoreDoc}.
     * @return The {@link SearchResult} defined by the specified {@link Document} and {@link ScoreDoc}.
     */
    public abstract SearchResult searchResult(Document document, ScoreDoc scoreDoc);

}
