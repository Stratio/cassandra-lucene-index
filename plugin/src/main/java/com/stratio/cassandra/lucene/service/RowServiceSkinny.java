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
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * {@link RowService} that manages simple rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RowServiceSkinny extends RowService {

    /** The names of the Lucene fields to be loaded. */
    private static final Set<String> FIELDS_TO_LOAD;

    static {
        FIELDS_TO_LOAD = new HashSet<>();
        FIELDS_TO_LOAD.add(PartitionKeyMapper.FIELD_NAME);
    }

    /** The used row mapper. */
    private final RowMapperSkinny mapper;

    /**
     * Returns a new {@code RowServiceSimple} to manage simple rows.
     *
     * @param cfs    The indexed {@link ColumnFamilyStore}.
     * @param config The {@link IndexConfig}.
     * @throws IOException If there are I/O errors.
     */
    public RowServiceSkinny(ColumnFamilyStore cfs, IndexConfig config) throws IOException {
        super(cfs, config);
        this.mapper = (RowMapperSkinny) super.mapper;
    }

    /**
     * {@inheritDoc}
     *
     * These fields are just the partition key.
     */
    @Override
    public Set<String> fieldsToLoad() {
        return FIELDS_TO_LOAD;
    }

    /** {@inheritDoc} */
    @Override
    public void doIndex(ByteBuffer key, ColumnFamily columnFamily, long timestamp) throws IOException {
        DecoratedKey partitionKey = mapper.partitionKey(key);
        if (columnFamily.iterator().hasNext()) {
            ColumnFamily cleanColumnFamily = cleanExpired(columnFamily, timestamp);
            lucene.upsert(documents(partitionKey, cleanColumnFamily, timestamp));
        } else if (columnFamily.deletionInfo() != null) {
            Term term = mapper.term(partitionKey);
            lucene.delete(term);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void doDelete(DecoratedKey partitionKey) throws IOException {
        Term term = mapper.term(partitionKey);
        lucene.delete(term);
    }

    /** {@inheritDoc} */
    @Override
    public Map<Term, Document> documents(DecoratedKey partitionKey, ColumnFamily columnFamily, long timestamp) {
        Columns columns = mapper.columns(partitionKey, columnFamily);
        if (!schema.mapsAll(columns)) {
            ColumnFamily completeColumnFamily = row(partitionKey, timestamp);
            columns = mapper.columns(partitionKey, completeColumnFamily);
        }
        Document document = mapper.document(partitionKey, columns);
        Term term = mapper.term(partitionKey);
        return Collections.singletonMap(term, document);
    }

    /** {@inheritDoc} */
    @Override
    protected List<Row> rows(List<SearchResult> searchResults, long timestamp, int scorePosition) {
        List<Row> rows = new ArrayList<>(searchResults.size());
        for (SearchResult searchResult : searchResults) {

            // Extract row from document
            DecoratedKey partitionKey = searchResult.getPartitionKey();
            ColumnFamily columnFamily = row(partitionKey, timestamp);
            if (columnFamily == null) {
                continue;
            }
            Row row = new Row(partitionKey, columnFamily);

            // Return decorated row
            if (scorePosition >= 0) {
                ScoreDoc scoreDoc = searchResult.getScoreDoc();
                row = addScoreColumn(row, timestamp, scoreDoc, scorePosition);
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * Returns the CQL3 {@link Row} identified by the specified key pair, using the specified time stamp to ignore
     * deleted columns. The {@link Row} is retrieved from the storage engine, so it involves IO operations.
     *
     * @param partitionKey The partition key.
     * @param timestamp    The time stamp to ignore deleted columns.
     * @return The CQL3 {@link Row} identified by the specified key pair.
     */
    private ColumnFamily row(DecoratedKey partitionKey, long timestamp) {
        QueryFilter queryFilter = QueryFilter.getIdentityFilter(partitionKey, metadata.cfName, timestamp);
        ColumnFamily columnFamily = baseCfs.getColumnFamily(queryFilter);
        if (columnFamily != null) {
            return cleanExpired(columnFamily, timestamp);
        }
        return null;
    }
}
