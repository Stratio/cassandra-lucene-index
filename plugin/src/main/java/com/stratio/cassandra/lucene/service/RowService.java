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
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.util.TimeCounter;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.SortField.FIELD_SCORE;

/**
 * Class for providing operations between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class RowService {

    private static final Logger logger = LoggerFactory.getLogger(RowService.class);

    /** The max number of rows to be read per iteration. */
    private static final int MAX_PAGE_SIZE = 100000;

    /** The default number of rows to be read per iteration. */
    private static final int FILTERING_PAGE_SIZE = 1000;

    final ColumnFamilyStore baseCfs;
    final RowMapper rowMapper;
    final CFMetaData metadata;
    final LuceneIndex luceneIndex;
    final List<SortField> keySortFields;

    protected final Schema schema;

    /**
     * Returns a new {@code RowService}.
     *
     * @param baseCfs          The base column family store.
     * @param columnDefinition The indexed column definition.
     * @throws IOException If there are I/O errors.
     */
    protected RowService(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) throws IOException {

        this.baseCfs = baseCfs;
        metadata = baseCfs.metadata;

        IndexConfig config = new IndexConfig(metadata, columnDefinition);

        schema = config.getSchema();
        rowMapper = RowMapper.build(metadata, columnDefinition, schema);
        keySortFields = rowMapper.sortFields();

        luceneIndex = new LuceneIndex(columnDefinition.ksName,
                                      columnDefinition.cfName,
                                      columnDefinition.getIndexName(),
                                      config.getPath(),
                                      config.getRamBufferMB(),
                                      config.getMaxMergeMB(),
                                      config.getMaxCachedMB(),
                                      config.getRefreshSeconds(),
                                      schema.getAnalyzer());
    }

    /**
     * Returns a new {@link RowService} for the specified {@link ColumnFamilyStore} and {@link ColumnDefinition}.
     *
     * @param baseCfs          The {@link ColumnFamilyStore} associated to the managed index.
     * @param columnDefinition The {@link ColumnDefinition} of the indexed column.
     * @return A new {@link RowService} for the specified {@link ColumnFamilyStore} and {@link ColumnDefinition}.
     * @throws IOException If there are I/O errors.
     */
    public static RowService build(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) throws IOException {
        int clusteringPosition = baseCfs.metadata.clusteringColumns().size();
        if (clusteringPosition > 0) {
            return new RowServiceWide(baseCfs, columnDefinition);
        } else {
            return new RowServiceSkinny(baseCfs, columnDefinition);
        }
    }

    /**
     * Returns the used {@link Schema}.
     *
     * @return The used {@link Schema}.
     */
    public final Schema getSchema() {
        return schema;
    }

    /**
     * Returns the names of the document fields to be loaded when reading a Lucene index.
     *
     * @return The names of the document fields to be loaded.
     */
    protected abstract Set<String> fieldsToLoad();

    /**
     * Indexes the logical {@link Row} identified by the specified key and column family using the specified time stamp.
     * The may require reading from the base {@link ColumnFamilyStore} because it could exist previously having more
     * columns than the specified ones. The specified {@link ColumnFamily} is used for determine the cluster key. This
     * operation is performed asynchronously.
     *
     * @param key          A partition key.
     * @param columnFamily A {@link ColumnFamily} with a single common cluster key.
     * @param timestamp    The insertion time.
     * @throws IOException If there are I/O errors.
     */
    public abstract void index(ByteBuffer key, ColumnFamily columnFamily, long timestamp) throws IOException;

    /**
     * Deletes the partition identified by the specified partition key.
     *
     * @param partitionKey The partition key identifying the partition to be deleted.
     * @throws IOException If there are I/O errors.
     */
    public abstract void delete(DecoratedKey partitionKey) throws IOException;

    /**
     * Deletes all the {@link Document}s.
     *
     * @throws IOException If there are I/O errors.
     */
    public final void truncate() throws IOException {
        luceneIndex.truncate();
    }

    /**
     * Closes and removes all the index files.
     *
     * @throws IOException If there are I/O errors.
     */
    public final void delete() throws IOException {
        luceneIndex.delete();
        schema.close();
    }

    /**
     * Commits the pending changes. This operation is performed asynchronously.
     *
     * @throws IOException If there are I/O errors.
     */
    public final void commit() throws IOException {
        luceneIndex.commit();
    }

    /**
     * Returns the Lucene {@link Document}s represented by the specified Cassandra row associated with their identifying
     * {@link Term}s.
     *
     * @param partitionKey A partition key.
     * @param columnFamily A column family.
     * @param timestamp    The operation time.
     * @return The Lucene {@link Document}s represented by the specified Cassandra row associated with their identifying
     * {@link Term}s.
     */
    public abstract Map<Term, Document> documents(DecoratedKey partitionKey, ColumnFamily columnFamily, long timestamp);

    /**
     * Returns the stored and indexed {@link Row}s satisfying the specified restrictions.
     *
     * @param search      The {@link Search} to be performed.
     * @param expressions A list of filtering {@link IndexExpression}s to be satisfied.
     * @param dataRange   A {@link DataRange} to be satisfied.
     * @param limit       The max number of {@link Row}s to be returned.
     * @param timestamp   The operation time stamp.
     * @param after       A {@link RowKey} to start the search after.
     * @return The {@link Row}s satisfying the specified restrictions.
     * @throws IOException If there are I/O errors.
     */
    public final List<Row> search(Search search,
                                  List<IndexExpression> expressions,
                                  DataRange dataRange,
                                  final int limit,
                                  long timestamp,
                                  RowKey after) throws IOException {
        logger.debug("Searching with search {} ", search);

        // Setup stats
        TimeCounter searchTime = TimeCounter.create().start();
        TimeCounter luceneTime = TimeCounter.create();
        TimeCounter collectTime = TimeCounter.create();
        int numDocs = 0;
        int numPages = 0;
        int numRows = 0;

        List<Row> rows = new LinkedList<>();

        // Refresh index if needed
        if (search.refresh()) {
            luceneIndex.refresh();
            if (search.isEmpty()) {
                return rows;
            }
        }

        SearcherManager searcherManager = luceneIndex.getSearcherManager();
        IndexSearcher searcher = searcherManager.acquire();
        try {

            // Setup search arguments
            Query query = query(search, dataRange);
            Sort sort = sort(search);
            int scorePosition = scorePosition(search);
            ScoreDoc last = after(searcher, after, query, sort);
            int page = Math.min(limit, MAX_PAGE_SIZE);
            boolean maybeMore;

            do {
                // Search rows identifiers in Lucene
                luceneTime.start();
                Set<String> fields = fieldsToLoad();
                Map<Document, ScoreDoc> docs = luceneIndex.search(searcher, query, sort, last, page, fields);
                List<SearchResult> searchResults = new ArrayList<>(docs.size());
                for (Map.Entry<Document, ScoreDoc> entry : docs.entrySet()) {
                    Document document = entry.getKey();
                    ScoreDoc scoreDoc = entry.getValue();
                    last = scoreDoc;
                    searchResults.add(rowMapper.searchResult(document, scoreDoc));
                }
                numDocs += searchResults.size();
                luceneTime.stop();

                // Collect rows from Cassandra
                collectTime.start();
                for (Row row : rows(searchResults, timestamp, scorePosition)) {
                    if (accepted(row, expressions)) {
                        rows.add(row);
                        numRows++;
                    }
                }
                collectTime.stop();

                // Setup next iteration
                maybeMore = searchResults.size() == page;
                page = Math.min(Math.max(FILTERING_PAGE_SIZE, numRows - limit), MAX_PAGE_SIZE);
                numPages++;

                // Iterate while there are still documents to read and we don't have enough rows
            } while (maybeMore && rows.size() < limit);

        } finally {
            searcherManager.release(searcher);
        }

        // Ensure sorting
        Comparator<Row> comparator = rowMapper.comparator(search);
        Collections.sort(rows, comparator);

        searchTime.stop();
        logger.debug("Lucene time: {}", luceneTime);
        logger.debug("Cassandra time: {}", collectTime);
        logger.debug("Collected {} docs and {} rows in {} pages in {}", numDocs, numRows, numPages, searchTime);

        return rows;
    }

    /**
     * Returns the {@link Query} representation of the specified {@link Search} filtered by the specified {@link
     * DataRange}.
     *
     * @param search    A {@link Search}.
     * @param dataRange A {@link DataRange}.
     * @return The {@link Query} representation of the specified {@link Search} filtered by the specified {@link
     * DataRange}.
     */
    public Query query(Search search, DataRange dataRange) {
        Query range = rowMapper.query(dataRange);
        Query query = search.query(schema);
        Query filter = search.filter(schema);
        if (query == null && filter == null && range == null) {
            return new MatchAllDocsQuery();
        }
        BooleanQuery booleanQuery = new BooleanQuery();
        if (range != null) {
            booleanQuery.add(range, FILTER);
        }
        if (filter != null) {
            booleanQuery.add(filter, FILTER);
        }
        if (query != null) {
            booleanQuery.add(query, MUST);
        }
        return new CachingWrapperQuery(booleanQuery);
    }

    /**
     * Returns a {@link Sort} for the specified {@link Search}.
     *
     * @param search A {@link Search}.
     * @return A {@link Sort} for the specified {@link Search}.
     */
    private Sort sort(Search search) {
        List<SortField> sortFields = new ArrayList<>();
        if (search.usesSorting()) {
            sortFields.addAll(search.sortFields(schema));
        }
        if (search.usesRelevance()) {
            sortFields.add(FIELD_SCORE);
        }
        sortFields.addAll(keySortFields);
        return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
    }

    private int scorePosition(Search search) {
        if (search.usesRelevance()) {
            return search.usesSorting() ? search.sortFields(schema).size() : 0;
        } else {
            return -1;
        }
    }

    /**
     * Returns the {@link ScoreDoc} of a previous search.
     *
     * @param searcher The Lucene {@link IndexSearcher} to be used.
     * @param key      The key of the last found row.
     * @param query    The previous query.
     * @param sort     The previous sort.
     * @return The {@link ScoreDoc} of a previous search.
     * @throws IOException If there are I/O errors.
     */
    private ScoreDoc after(IndexSearcher searcher, RowKey key, Query query, Sort sort) throws IOException {
        TimeCounter time = TimeCounter.create().start();
        if (key == null) {
            return null;
        }
        Filter rowFilter = new QueryWrapperFilter(rowMapper.query(key));
        Query afterQuery = new FilteredQuery(query, rowFilter);
        Set<String> fields = Collections.emptySet();
        Map<Document, ScoreDoc> results = luceneIndex.search(searcher, afterQuery, sort, null, 1, fields);
        ScoreDoc scoreDoc = results.isEmpty() ? null : results.values().iterator().next();
        logger.debug("Search after time: {}", time.stop());
        return scoreDoc;
    }

    /**
     * Returns {@code true} if the specified {@link Row} satisfies the all the specified {@link IndexExpression}s,
     * {@code false} otherwise.
     *
     * @param row         A {@link Row}.
     * @param expressions A list of {@link IndexExpression}s to be satisfied by {@code row}.
     * @return {@code true} if the specified {@link Row} satisfies the all the specified {@link IndexExpression}s,
     * {@code false} otherwise.
     */
    private boolean accepted(Row row, List<IndexExpression> expressions) {
        if (!expressions.isEmpty()) {
            Columns columns = rowMapper.columns(row);
            for (IndexExpression expression : expressions) {
                if (!accepted(columns, expression)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns {@code true} if the specified {@link Columns} satisfies the the specified {@link IndexExpression}, {@code
     * false} otherwise.
     *
     * @param columns    A {@link Columns}.
     * @param expression A {@link IndexExpression}s to be satisfied by {@code columns}.
     * @return {@code true} if the specified {@link Columns} satisfies the the specified {@link IndexExpression}, {@code
     * false} otherwise.
     */
    private boolean accepted(Columns columns, IndexExpression expression) {

        ColumnDefinition def = metadata.getColumnDefinition(expression.column);
        String name = def.name.toString();
        ByteBuffer expectedValue = expression.value;
        Operator operator = expression.operator;
        AbstractType<?> validator = def.type;

        for (Column<?> column : columns.getColumnsByName(name)) {
            if (accepted(column, validator, operator, expectedValue)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if the specified {@link Column} satisfies the the specified expression, {@code false}
     * otherwise.
     *
     * @param column    A {@link Column}.
     * @param validator An expression validator.
     * @param operator  An expression operator.
     * @param value     An expression value.
     * @return {@code true} if the specified {@link Column} satisfies the the specified expression, {@code false}
     * otherwise.
     */
    private boolean accepted(Column<?> column, AbstractType<?> validator, Operator operator, ByteBuffer value) {

        if (column == null) {
            return false;
        }

        ByteBuffer actualValue = column.getDecomposedValue();
        if (actualValue == null) {
            return false;
        }

        int comparison = validator.compare(actualValue, value);
        return accepted(operator, comparison);
    }

    private boolean accepted(Operator operator, int comparison) {
        switch (operator) {
            case EQ:
                return comparison == 0;
            case GTE:
                return comparison >= 0;
            case GT:
                return comparison > 0;
            case LTE:
                return comparison <= 0;
            case LT:
                return comparison < 0;
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Returns the {@link Row}s identified by the specified {@link Document}s, using the specified time stamp to ignore
     * deleted columns. The {@link Row}s are retrieved from the storage engine, so it involves IO operations.
     *
     * @param results       The {@link SearchResult}s
     * @param timestamp     The time stamp to ignore deleted columns.
     * @param scorePosition The position where score column is placed.
     * @return The {@link Row}s identified by the specified {@link Document}s
     */
    protected abstract List<Row> rows(List<SearchResult> results, long timestamp, int scorePosition);

    /**
     * Returns a {@link ColumnFamily} composed by the non expired {@link Cell}s of the specified  {@link ColumnFamily}.
     *
     * @param columnFamily A {@link ColumnFamily}.
     * @param timestamp    The max allowed timestamp for the {@link Cell}s.
     * @return A {@link ColumnFamily} composed by the non expired {@link Cell}s of the specified  {@link ColumnFamily}.
     */
    protected ColumnFamily cleanExpired(ColumnFamily columnFamily, long timestamp) {
        ColumnFamily cleanColumnFamily = ArrayBackedSortedColumns.factory.create(baseCfs.metadata);
        for (Cell cell : columnFamily) {
            if (cell.isLive(timestamp)) {
                cleanColumnFamily.addColumn(cell);
            }
        }
        return cleanColumnFamily;
    }

    /**
     * Adds to the specified {@link Row} the specified Lucene score column.
     *
     * @param row           A {@link Row}.
     * @param timestamp     The score column timestamp.
     * @param scoreDoc      The score column value.
     * @param scorePosition The position where score column is placed.
     * @return The {@link Row} with the score.
     */
    protected Row addScoreColumn(Row row, long timestamp, ScoreDoc scoreDoc, int scorePosition) {

        ColumnFamily cf = row.cf;
        CellName cellName = rowMapper.makeCellName(cf);
        FieldDoc fieldDoc = (FieldDoc) scoreDoc;
        Float score = Float.parseFloat(fieldDoc.fields[scorePosition].toString());

        ColumnFamily dcf = ArrayBackedSortedColumns.factory.create(baseCfs.metadata);
        ByteBuffer cellValue = UTF8Type.instance.decompose(score.toString());
        dcf.addColumn(cellName, cellValue, timestamp);
        dcf.addAll(row.cf);

        return new Row(row.key, dcf);
    }

    /**
     * Returns the used {@link RowMapper}.
     *
     * @return The used {@link RowMapper}.
     */
    public RowMapper mapper() {
        return rowMapper;
    }
}
