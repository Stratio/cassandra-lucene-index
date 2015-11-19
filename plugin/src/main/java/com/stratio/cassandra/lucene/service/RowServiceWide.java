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

import com.google.common.collect.Lists;
import com.stratio.cassandra.lucene.IndexConfig;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

/**
 * {@link RowService} that manages wide rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RowServiceWide extends RowService {

    /** The names of the Lucene fields to be loaded. */
    private static final Set<String> FIELDS_TO_LOAD;

    static {
        FIELDS_TO_LOAD = new HashSet<>();
        FIELDS_TO_LOAD.add(PartitionKeyMapper.FIELD_NAME);
        FIELDS_TO_LOAD.add(ClusteringKeyMapper.FIELD_NAME);
    }

    private static final int ROWS_PER_SLICE_QUERY = 1000;

    /** The used row mapper. */
    private final RowMapperWide mapper;

    /**
     * Returns a new {@code RowServiceWide} to manage wide rows.
     *
     * @param cfs    The indexed {@link ColumnFamilyStore}.
     * @param config The {@link IndexConfig}.
     * @throws IOException If there are I/O errors.
     */
    public RowServiceWide(ColumnFamilyStore cfs, IndexConfig config) throws IOException {
        super(cfs, config);
        this.mapper = (RowMapperWide) super.mapper;
    }

    /**
     * {@inheritDoc}
     *
     * These fields are the partition and clustering keys.
     */
    @Override
    public Set<String> fieldsToLoad() {
        return FIELDS_TO_LOAD;
    }

    /** {@inheritDoc} */
    @Override
    public void doIndex(ByteBuffer key, ColumnFamily columnFamily, long timestamp) throws IOException {
        DeletionInfo deletionInfo = columnFamily.deletionInfo();
        DecoratedKey partitionKey = mapper.partitionKey(key);

        if (columnFamily.iterator().hasNext()) {
            ColumnFamily cleanColumnFamily = cleanExpired(columnFamily, timestamp);
            lucene.upsert(documents(partitionKey, cleanColumnFamily, timestamp));
        } else if (deletionInfo != null) {
            Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator();
            if (iterator.hasNext()) {
                while (iterator.hasNext()) {
                    RangeTombstone rangeTombstone = iterator.next();
                    Query query = mapper.query(partitionKey, rangeTombstone);
                    lucene.delete(query);
                }
            } else {
                Term term = mapper.term(partitionKey);
                lucene.delete(term);
            }
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

        Map<CellName, ColumnFamily> incomingRows = mapper.splitRows(columnFamily);
        Map<Term, Document> documents = new HashMap<>(incomingRows.size());
        List<CellName> incompleteRows = new ArrayList<>(incomingRows.size());

        // Separate complete and incomplete rows
        for (Map.Entry<CellName, ColumnFamily> entry : incomingRows.entrySet()) {
            CellName clusteringKey = entry.getKey();
            ColumnFamily rowColumnFamily = entry.getValue();
            Columns columns = mapper.columns(partitionKey, rowColumnFamily);
            if (schema.mapsAll(columns)) {
                Term term = mapper.term(partitionKey, clusteringKey);
                Document document = mapper.document(partitionKey, clusteringKey, columns);
                documents.put(term, document);
            } else {
                incompleteRows.add(clusteringKey);
            }
        }

        // Read incomplete rows from Cassandra storage engine
        if (!incompleteRows.isEmpty()) {
            for (Entry<CellName, ColumnFamily> entry : rows(partitionKey, incompleteRows, timestamp).entrySet()) {
                CellName clusteringKey = entry.getKey();
                ColumnFamily rowColumnFamily = entry.getValue();
                Columns columns = mapper.columns(partitionKey, rowColumnFamily);
                Term term = mapper.term(partitionKey, clusteringKey);
                Document document = mapper.document(partitionKey, clusteringKey, columns);
                documents.put(term, document);
            }
        }
        return documents;
    }

    /**
     * {@inheritDoc}
     *
     * The {@link Row} is a logical one.
     */
    @Override
    protected List<Row> rows(List<SearchResult> searchResults, long timestamp, int scorePosition) {

        // Group key queries by partition keys
        Map<String, ScoreDoc> scoresByClusteringKey = new HashMap<>(searchResults.size());
        Map<DecoratedKey, List<CellName>> keys = new HashMap<>();
        for (SearchResult searchResult : searchResults) {
            DecoratedKey partitionKey = searchResult.getPartitionKey();
            CellName clusteringKey = searchResult.getClusteringKey();
            ScoreDoc scoreDoc = searchResult.getScoreDoc();
            String rowHash = mapper.hash(partitionKey, clusteringKey);
            scoresByClusteringKey.put(rowHash, scoreDoc);
            List<CellName> clusteringKeys = keys.get(partitionKey);
            if (clusteringKeys == null) {
                clusteringKeys = new ArrayList<>();
                keys.put(partitionKey, clusteringKeys);
            }
            clusteringKeys.add(clusteringKey);
        }

        List<Row> rows = new ArrayList<>(searchResults.size());
        for (Map.Entry<DecoratedKey, List<CellName>> entry : keys.entrySet()) {
            DecoratedKey partitionKey = entry.getKey();
            for (List<CellName> clusteringKeys : Lists.partition(entry.getValue(), ROWS_PER_SLICE_QUERY)) {
                Map<CellName, ColumnFamily> partitionRows = rows(partitionKey, clusteringKeys, timestamp);
                for (Map.Entry<CellName, ColumnFamily> entry1 : partitionRows.entrySet()) {
                    CellName clusteringKey = entry1.getKey();
                    ColumnFamily columnFamily = entry1.getValue();
                    Row row = new Row(partitionKey, columnFamily);
                    if (scorePosition >= 0) {
                        String rowHash = mapper.hash(partitionKey, clusteringKey);
                        ScoreDoc scoreDoc = scoresByClusteringKey.get(rowHash);
                        row = addScoreColumn(row, timestamp, scoreDoc, scorePosition);
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    /**
     * Returns the CQL3 {@link Row} identified by the specified key pair, using the specified time stamp to ignore
     * deleted columns. The {@link Row} is retrieved from the storage engine, so it involves IO operations.
     *
     * @param partitionKey   The partition key.
     * @param clusteringKeys The clustering keys.
     * @param timestamp      The time stamp to ignore deleted columns.
     * @return The CQL3 {@link Row} identified by the specified key pair.
     */
    private Map<CellName, ColumnFamily> rows(DecoratedKey partitionKey, List<CellName> clusteringKeys, long timestamp) {
        ColumnSlice[] slices = mapper.columnSlices(clusteringKeys);

        if (baseCfs.metadata.hasStaticColumns()) {
            LinkedList<ColumnSlice> l = new LinkedList<>(Arrays.asList(slices));
            l.addFirst(baseCfs.metadata.comparator.staticPrefix().slice());
            slices = new ColumnSlice[l.size()];
            slices = l.toArray(slices);
        }

        int compositesToGroup = baseCfs.metadata.clusteringColumns().size();
        SliceQueryFilter dataFilter = new SliceQueryFilter(slices, false, Integer.MAX_VALUE, compositesToGroup);
        QueryFilter queryFilter = new QueryFilter(partitionKey, baseCfs.name, dataFilter, timestamp);

        ColumnFamily queryColumnFamily = baseCfs.getColumnFamily(queryFilter);

        // Avoid null
        if (queryColumnFamily == null) {
            return Collections.emptyMap();
        }

        // Remove deleted/expired columns
        ColumnFamily cleanQueryColumnFamily = cleanExpired(queryColumnFamily, timestamp);

        // Split and return CQL3 row column families
        return mapper.splitRows(cleanQueryColumnFamily);
    }

}
