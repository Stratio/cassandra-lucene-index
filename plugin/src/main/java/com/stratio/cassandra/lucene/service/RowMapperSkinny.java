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

import com.stratio.cassandra.lucene.IndexConfig;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

/**
 * {@link RowMapper} for skinny rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RowMapperSkinny extends RowMapper {

    /**
     * Builds a new {@link RowMapperSkinny} for the specified {@link IndexConfig}.
     *
     * @param config The {@link IndexConfig}.
     */
    RowMapperSkinny(IndexConfig config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override
    public Columns columns(DecoratedKey partitionKey, ColumnFamily columnFamily) {
        Columns columns = new Columns();
        columns.add(partitionKeyMapper.columns(partitionKey));
        columns.add(regularCellsMapper.columns(columnFamily));
        return columns;
    }

    /**
     * Returns a Lucene {@link Document} representing the logical CQL row represented by the specified partition key and
     * {@link Columns}.
     *
     * @param partitionKey The partition key of the logical CQL row.
     * @param columns      The {@link Columns} of the logical CQL row.
     * @return A Lucene {@link Document} representing the specified logical CQL row
     */
    public Document document(DecoratedKey partitionKey, Columns columns) {
        Document document = new Document();
        tokenMapper.addFields(document, partitionKey);
        partitionKeyMapper.addFields(document, partitionKey);
        schema.addFields(document, columns);
        return document;
    }

    /** {@inheritDoc} */
    @Override
    public List<SortField> sortFields() {
        return tokenMapper.sortFields();
    }

    /** {@inheritDoc} */
    @Override
    public final Query query(DataRange dataRange) {
        RowPosition startPosition = dataRange.startKey();
        RowPosition stopPosition = dataRange.stopKey();
        Token startToken = startPosition.getToken();
        Token stopToken = stopPosition.getToken();
        boolean includeStart = tokenMapper.includeStart(startPosition);
        boolean includeStop = tokenMapper.includeStop(stopPosition);
        if (startPosition instanceof DecoratedKey) {
            DecoratedKey decoratedKey = (DecoratedKey) startPosition;
            IDiskAtomFilter filter = dataRange.columnFilter(decoratedKey.getKey());
            if (filter instanceof SliceQueryFilter) {
                SliceQueryFilter sliceQueryFilter = (SliceQueryFilter) filter;
                Composite startName = sliceQueryFilter.start();
                if (startName != null && !startName.isEmpty()) {
                    includeStart = false;
                }
            }
        }
        return tokenMapper.query(startToken, stopToken, includeStart, includeStop);
    }

    /** {@inheritDoc} */
    @Override
    public Query query(RowKey rowKey) {
        DecoratedKey partitionKey = rowKey.getPartitionKey();
        Term term = term(partitionKey);
        return new TermQuery(term);
    }

    /** {@inheritDoc} */
    @Override
    public CellName makeCellName(ColumnFamily columnFamily) {
        return metadata.comparator.makeCellName(columnDefinition.name.bytes);
    }

    /** {@inheritDoc} */
    @Override
    public Comparator<Row> comparator() {
        return tokenMapper.comparator();
    }

    /** {@inheritDoc} */
    @Override
    public SearchResult searchResult(Document document, ScoreDoc scoreDoc) {
        DecoratedKey partitionKey = partitionKeyMapper.partitionKey(document);
        return new SearchResult(partitionKey, null, scoreDoc);
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer byteBuffer(RowKey rowKey) {
        return rowKey.getPartitionKey().getKey();
    }

    /** {@inheritDoc} */
    @Override
    public RowKey rowKey(ByteBuffer bb) {
        DecoratedKey partitionKey = partitionKey(bb);
        return new RowKey(partitionKey, null);
    }

    /** {@inheritDoc} */
    @Override
    public RowKey rowKey(Row row) {
        DecoratedKey partitionKey = row.key;
        return new RowKey(partitionKey, null);
    }
}
