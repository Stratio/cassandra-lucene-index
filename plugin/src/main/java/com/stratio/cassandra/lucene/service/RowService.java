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
import com.stratio.cassandra.lucene.util.TaskQueue;
import com.stratio.cassandra.lucene.util.TimeCounter;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
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
import java.util.*;

import static java.lang.Math.max;
import static java.lang.Math.min;
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
    private static final int MAX_PAGE_SIZE = 10000;

    /** The min number of rows to be read per iteration. */
    private static final int MIN_PAGE_SIZE = 100;

    final ColumnFamilyStore baseCfs;
    final CFMetaData metadata;
    final RowMapper mapper;
    final LuceneIndex lucene;
    final List<SortField> keySortFields;

    protected final Schema schema;
    private final TaskQueue indexQueue;

    /**
     * Returns a new {@code RowService} for the specified {@link IndexConfig}.
     *
     * @param cfs    The indexed {@link ColumnFamilyStore}.
     * @param config The {@link IndexConfig}.
     * @throws IOException If there are I/O errors.
     */
    protected RowService(ColumnFamilyStore cfs, IndexConfig config) throws IOException {
        baseCfs = cfs;
        metadata = config.getMetadata();
        schema = config.getSchema();
        lucene = new LuceneIndex(config);
        mapper = RowMapper.build(config);
        keySortFields = mapper.sortFields();

        int threads = config.getIndexingThreads();
        indexQueue = threads > 0 ? new TaskQueue(threads, config.getIndexingQueuesSize()) : null;
    }

    /**
     * Returns a new {@link RowService} for the specified {@link IndexConfig}.
     *
     * @param cfs    The indexed {@link ColumnFamilyStore}.
     * @param config The {@link IndexConfig}.
     * @return A new {@link RowService} for the specified {@link IndexConfig}.
     * @throws IOException If there are I/O errors.
     */
    public static RowService build(ColumnFamilyStore cfs, IndexConfig config) throws IOException {
        return config.isWide() ? new RowServiceWide(cfs, config) : new RowServiceSkinny(cfs, config);
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
    public void index(final ByteBuffer key, final ColumnFamily columnFamily, final long timestamp) throws IOException {
        if (indexQueue == null) {
            doIndex(key, columnFamily, timestamp);
        } else {
            indexQueue.submitAsynchronous(key, new Runnable() {
                @Override
                public void run() {
                    try {
                        doIndex(key, columnFamily, timestamp);
                    } catch (Exception e) {
                        logger.error("Unrecoverable error during asynchronously indexing", e);
                    }
                }
            });
        }
    }

    /**
     * Puts in the Lucene index the Cassandra's the row identified by the specified partition key and the clustering
     * keys contained in the specified {@link ColumnFamily}.
     *
     * @param key          A partition key.
     * @param columnFamily A {@link ColumnFamily} with a single common cluster key.
     * @param timestamp    The insertion time.
     * @throws IOException If there are I/O errors.
     */
    protected abstract void doIndex(ByteBuffer key, ColumnFamily columnFamily, long timestamp) throws IOException;

    /**
     * Deletes the partition identified by the specified partition key. This operation is performed asynchronously.
     *
     * @param partitionKey The partition key identifying the partition to be deleted.
     * @throws IOException If there are I/O errors.
     */
    public void delete(final DecoratedKey partitionKey) throws IOException {
        if (indexQueue == null) {
            doDelete(partitionKey);
        } else {
            indexQueue.submitAsynchronous(partitionKey, new Runnable() {
                @Override
                public void run() {
                    try {
                        doDelete(partitionKey);
                    } catch (Exception e) {
                        logger.error("Unrecoverable error during asynchronous deletion", e);
                    }
                }
            });
        }
    }

    /**
     * Deletes the partition identified by the specified partition key.
     *
     * @param partitionKey The partition key identifying the partition to be deleted.
     * @throws IOException If there are I/O errors.
     */
    protected abstract void doDelete(DecoratedKey partitionKey) throws IOException;

    /**
     * Deletes all the {@link Document}s.
     *
     * @throws IOException If there are I/O errors.
     */
    public final void truncate() throws IOException {
        lucene.truncate();
    }

    /**
     * Closes and removes all the index files.
     *
     * @throws IOException If there are I/O errors.
     */
    public final void delete() throws IOException {
        if (indexQueue != null) {
            indexQueue.shutdown();
        }
        lucene.delete();
        schema.close();
    }

    /**
     * Commits the pending changes. This operation is performed asynchronously.
     *
     * @throws IOException If there are I/O errors.
     */
    public final void commit() throws IOException {
        if (indexQueue != null) {
            indexQueue.await();
        }
        lucene.commit();
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
    abstract Map<Term, Document> documents(DecoratedKey partitionKey, ColumnFamily columnFamily, long timestamp);

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

        // Setup stats
        TimeCounter afterTime = TimeCounter.create();
        TimeCounter queryTime = TimeCounter.create();
        TimeCounter storeTime = TimeCounter.create();
        int numDocs = 0;
        int numPages = 0;
        int numRows = 0;

        List<Row> rows = new LinkedList<>();

        // Refresh index if needed
        if (search.refresh()) {
            if (indexQueue != null) {
                indexQueue.await();
            }
            lucene.refresh();
            if (search.isEmpty()) {
                return rows;
            }
        }

        // Setup search
        Query query = query(search, dataRange);
        Sort sort = sort(search);

        // Setup paging
        int scorePosition = scorePosition(search);
        int page = min(limit, MAX_PAGE_SIZE);
        boolean mayBeMoreDocs;
        int remainingRows;

        SearcherManager searcherManager = lucene.getSearcherManager();
        IndexSearcher searcher = searcherManager.acquire();
        try {

            // Get last position
            afterTime.start();
            ScoreDoc last = after(searcher, after, query, sort);
            afterTime.stop();

            do {
                // Search rows identifiers in Lucene
                queryTime.start();
                Set<String> fields = fieldsToLoad();
                Map<Document, ScoreDoc> docs = lucene.search(searcher, query, sort, last, page, fields);
                List<SearchResult> searchResults = new ArrayList<>(docs.size());
                for (Map.Entry<Document, ScoreDoc> entry : docs.entrySet()) {
                    Document document = entry.getKey();
                    ScoreDoc scoreDoc = entry.getValue();
                    last = scoreDoc;
                    searchResults.add(mapper.searchResult(document, scoreDoc));
                }
                numDocs += searchResults.size();
                queryTime.stop();

                // Collect rows from Cassandra
                storeTime.start();
                for (Row row : rows(searchResults, timestamp, scorePosition)) {
                    if (accepted(row, expressions)) {
                        rows.add(row);
                        numRows++;
                    }
                }
                storeTime.stop();

                // Setup next iteration
                mayBeMoreDocs = searchResults.size() == page;
                remainingRows = limit - numRows;
                page = min(max(MIN_PAGE_SIZE, remainingRows), MAX_PAGE_SIZE);
                numPages++;

                // Iterate while there are still documents to read and we don't have enough rows
            } while (mayBeMoreDocs && remainingRows > 0);

        } finally {
            searcherManager.release(searcher);
        }

        // Ensure sorting
        Comparator<Row> comparator = mapper.comparator(search);
        Collections.sort(rows, comparator);

        logger.debug("Search     : {}", search);
        logger.debug("Query      : {}", query);
        logger.debug("Sort       : {}", sort);
        logger.debug("After time : {}", afterTime);
        logger.debug("Query time : {}", queryTime);
        logger.debug("Store time : {}", storeTime);
        logger.debug("Count docs : {}", numDocs);
        logger.debug("Count rows : {}", numRows);
        logger.debug("Count page : {}", numPages);

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
        Query range = mapper.query(dataRange);
        Query query = search.query(schema);
        Query filter = search.filter(schema);
        if (query == null && filter == null && range == null) {
            return new MatchAllDocsQuery();
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (range != null) {
            builder.add(range, FILTER);
        }
        if (filter != null) {
            builder.add(filter, FILTER);
        }
        if (query != null) {
            builder.add(query, MUST);
        }
        return new CachingWrapperQuery(builder.build());
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
        if (key == null) {
            return null;
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(mapper.query(key), FILTER);
        builder.add(query, MUST);
        Query afterQuery = builder.build();

        Set<String> fields = Collections.emptySet();
        Map<Document, ScoreDoc> results = lucene.search(searcher, afterQuery, sort, null, 1, fields);
        return results.isEmpty() ? null : results.values().iterator().next();
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
            Columns columns = mapper.columns(row);
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
        CellName cellName = mapper.makeCellName(cf);
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
        return mapper;
    }
}
