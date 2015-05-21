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

import com.stratio.cassandra.lucene.IndexConfig;
import com.stratio.cassandra.lucene.query.Search;
import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.util.Log;
import com.stratio.cassandra.lucene.util.TaskQueue;
import com.stratio.cassandra.lucene.util.TimeCounter;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class for mapping rows between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class RowService {

    /** The max number of rows to be read per iteration. */
    private static final int MAX_PAGE_SIZE = 100000;
    private static final int FILTERING_PAGE_SIZE = 1000;

    final ColumnFamilyStore baseCfs;
    final RowMapper rowMapper;
    final CFMetaData metadata;
    final LuceneIndex luceneIndex;

    private final Schema schema;
    private final TaskQueue indexQueue;

    /**
     * Returns a new {@code RowService}.
     *
     * @param baseCfs          The base column family store.
     * @param columnDefinition The indexed column definition.
     */
    protected RowService(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) {

        this.baseCfs = baseCfs;
        this.metadata = baseCfs.metadata;

        IndexConfig config = new IndexConfig(metadata, columnDefinition.getIndexOptions());

        this.schema = config.getSchema();
        this.rowMapper = RowMapper.build(metadata, columnDefinition, schema);

        this.luceneIndex = new LuceneIndex(config.getPath(),
                                           config.getRefreshSeconds(),
                                           config.getRamBufferMB(),
                                           config.getMaxMergeMB(),
                                           config.getMaxCachedMB(),
                                           schema.getAnalyzer());

        int indexingThreads = config.getIndexingThreads();
        if (indexingThreads > 0) {
            this.indexQueue = new TaskQueue(indexingThreads, config.getIndexingQueuesSize());
        } else {
            this.indexQueue = null;
        }
    }

    /**
     * Returns a new {@link RowService} for the specified {@link ColumnFamilyStore} and {@link ColumnDefinition}.
     *
     * @param baseCfs          The {@link ColumnFamilyStore} associated to the managed index.
     * @param columnDefinition The {@link ColumnDefinition} of the indexed column.
     * @return A new {@link RowService} for the specified {@link ColumnFamilyStore} and {@link ColumnDefinition}.
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
     * The must be read from the {@link ColumnFamilyStore} because it could exist previously having more columns than
     * the specified ones. The specified {@link ColumnFamily} is used for determine the cluster key. This operation is
     * performed asynchronously.
     *
     * @param key          A partition key.
     * @param columnFamily A {@link ColumnFamily} with a single common cluster key.
     * @param timestamp    The insertion time.
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
                        Log.error(e, "Unrecoverable error during asynchronously indexing");
                    }
                }
            });
        }
    }

    /**
     * Puts in the Lucene index the Cassandra's the row identified by the specified partition key and the clustering
     * keys contained in the specified {@link ColumnFamily}.
     *
     * @param key          The partition key.
     * @param columnFamily The column family containing the clustering keys.
     * @param timestamp    The operation time stamp.
     */
    protected abstract void doIndex(ByteBuffer key, ColumnFamily columnFamily, long timestamp) throws IOException;

    /**
     * Deletes the partition identified by the specified partition key. This operation is performed asynchronously.
     *
     * @param partitionKey The partition key identifying the partition to be deleted.
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
                        Log.error(e, "Unrecoverable error during asynchronous deletion of %s", partitionKey);
                    }
                }
            });
        }
    }

    /**
     * Deletes the partition identified by the specified partition key.
     *
     * @param partitionKey The partition key identifying the partition to be deleted.
     */
    protected abstract void doDelete(DecoratedKey partitionKey) throws IOException;

    /**
     * Deletes all the {@link Document}s.
     */
    public final void truncate() throws IOException {
        luceneIndex.truncate();
    }

    /**
     * Closes and removes all the index files.
     */
    public final void delete() throws IOException {
        luceneIndex.delete();
        schema.close();
    }

    /**
     * Commits the pending changes. This operation is performed asynchronously.
     */
    public final void commit() throws IOException {
        if (indexQueue == null) {
            luceneIndex.commit();
        } else {
            indexQueue.submitSynchronous(new Runnable() {
                @Override
                public void run() {
                    try {
                        luceneIndex.commit();
                    } catch (Exception e) {
                        Log.error(e, "Unrecoverable error during asynchronous commit");
                    }
                }
            });
        }
    }

    /**
     * Returns the stored and indexed {@link Row}s satisfying the specified restrictions.
     *
     * @param search      The {@link Search} to be performed.
     * @param expressions A list of filtering {@link IndexExpression}s to be satisfied.
     * @param dataRange   A {@link DataRange} to be satisfied.
     * @param limit       The max number of {@link Row}s to be returned.
     * @param timestamp   The operation time stamp.
     * @return The {@link Row}s satisfying the specified restrictions.
     */
    public final List<Row> search(Search search,
                                  List<IndexExpression> expressions,
                                  DataRange dataRange,
                                  final int limit,
                                  long timestamp) throws IOException {
        Log.debug("Searching with search %s ", search);

        // Setup stats
        TimeCounter searchTime = new TimeCounter();
        TimeCounter luceneTime = new TimeCounter();
        TimeCounter collectTime = new TimeCounter();
        TimeCounter sortTime = new TimeCounter();
        int numDocs = 0;
        int numPages = 0;

        searchTime.start();

        // Setup search arguments
        Query rangeQuery = rowMapper.query(dataRange);
        Query query = search.query(schema, rangeQuery);
        Sort sort = search.sort(schema);
        boolean relevance = search.usesRelevance();

        // Setup search pagination
        List<Row> rows = new LinkedList<>(); // The row list to be returned
        ScoreDoc last = null; // The last search result

        // Paginate search collecting documents
        int page = Math.min(limit, MAX_PAGE_SIZE);
        boolean maybeMore;
        do {
            // Search rows identifiers in Lucene
            luceneTime.start();
            Map<Document, ScoreDoc> docs = luceneIndex.search(query, sort, last, page, fieldsToLoad(), relevance);
            List<SearchResult> searchResults = new ArrayList<>(docs.size());
            for (Map.Entry<Document, ScoreDoc> entry : docs.entrySet()) {
                searchResults.add(rowMapper.searchResult(entry.getKey(), entry.getValue()));
            }
            numDocs += searchResults.size();
            last = searchResults.isEmpty() ? null : searchResults.get(searchResults.size() - 1).getScoreDoc();
            luceneTime.stop();

            // Collect rows from Cassandra
            collectTime.start();
            for (Row row : rows(searchResults, timestamp, relevance)) {
                if (row != null && accepted(row, expressions)) {
                    rows.add(row);
                }
            }
            collectTime.stop();

            // Setup next iteration
            maybeMore = searchResults.size() == page;
            page = Math.min(Math.max(FILTERING_PAGE_SIZE, rows.size() - limit), MAX_PAGE_SIZE);
            numPages++;

            // Iterate while there are still documents to read and we don't have enough rows
        } while (maybeMore && rows.size() < limit);

        sortTime.start();
        Collections.sort(rows, comparator());
        sortTime.stop();

        searchTime.stop();

        Log.debug("Lucene time: %s", luceneTime);
        Log.debug("Cassandra time: %s", collectTime);
        Log.debug("Sort time: %s", sortTime);
        Log.debug("Collected %d docs and %d rows in %d pages in %s", numDocs, rows.size(), numPages, searchTime);

        return rows;
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
     * @param columns    A {@link Columns}
     * @param expression A {@link IndexExpression}s to be satisfied by {@code columns}.
     * @return {@code true} if the specified {@link Columns} satisfies the the specified {@link IndexExpression}, {@code
     * false} otherwise.
     */
    private boolean accepted(Columns columns, IndexExpression expression) {

        ByteBuffer expectedValue = expression.value;

        ColumnDefinition def = metadata.getColumnDefinition(expression.column);
        String name = def.name.toString();

        Column column = columns.getColumn(name);
        if (column == null) {
            return false;
        }

        ByteBuffer actualValue = column.getDecomposedValue();
        if (actualValue == null) {
            return false;
        }

        AbstractType<?> validator = def.type;
        int comparison = validator.compare(actualValue, expectedValue);
        switch (expression.operator) {
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
     * @param searchResults The {@link SearchResult}s
     * @param timestamp     The time stamp to ignore deleted columns.
     * @param usesRelevance If the search uses relevance.
     * @return The {@link Row} identified by the specified {@link Document}s
     */
    protected abstract List<Row> rows(List<SearchResult> searchResults, long timestamp, boolean usesRelevance);

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
     * @param row       A {@link Row}.
     * @param timestamp The score column timestamp.
     * @param score     The score column value.
     * @return The {@link Row} with the score.
     */
    protected Row addScoreColumn(Row row, long timestamp, Float score) {
        ColumnFamily cf = row.cf;
        CellName cellName = rowMapper.makeCellName(cf);
        ByteBuffer cellValue = UTF8Type.instance.decompose(score.toString());

        ColumnFamily dcf = ArrayBackedSortedColumns.factory.create(baseCfs.metadata);
        dcf.addColumn(cellName, cellValue, timestamp);
        dcf.addAll(row.cf);

        return new Row(row.key, dcf);
    }

    /**
     * Returns the {@link RowComparator} to be used for ordering the {@link Row}s obtained from the specified {@link
     * Search}. This {@link Comparator} is useful for merging the partial results obtained from running the specified
     * {@link Search} against several indexes.
     *
     * @param search A {@link Search}.
     * @return The {@link RowComparator} to be used for ordering the {@link Row}s obtained from the specified {@link
     * Search}.
     */
    public RowComparator comparator(Search search) {
        if (search != null) {
            if (search.usesSorting()) // Sort with search itself
            {
                return new RowComparatorSorting(rowMapper, search.getSort());
            } else if (search.usesRelevance()) // Sort with row's score
            {
                return new RowComparatorScoring(this);
            }
        }
        return rowMapper.naturalComparator();
    }

    /**
     * Returns the default {@link Row} comparator. This comparator is based on Cassandra's natural order.
     *
     * @return The default {@link Row} comparator.
     */
    public RowComparator comparator() {
        return rowMapper.naturalComparator();
    }

    /**
     * Returns the score of the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return The score of the specified {@link Row}.
     */
    protected Float score(Row row) {
        ColumnFamily cf = row.cf;
        CellName cellName = rowMapper.makeCellName(cf);
        Cell cell = cf.getColumn(cellName);
        String value = UTF8Type.instance.compose(cell.value());
        return Float.parseFloat(value);
    }

    /**
     * Returns the total number of {@link Document}s in the index.
     *
     * @return The total number of {@link Document}s in the index.
     */
    public long getIndexSize() throws IOException {
        return luceneIndex.getNumDocs();
    }

}
