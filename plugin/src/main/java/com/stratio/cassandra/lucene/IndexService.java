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

package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.cache.SearchCache;
import com.stratio.cassandra.lucene.cache.SearchCacheEntry;
import com.stratio.cassandra.lucene.cache.SearchCacheUpdater;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.column.ColumnsMapper;
import com.stratio.cassandra.lucene.index.DocumentIterator;
import com.stratio.cassandra.lucene.index.FSIndex;
import com.stratio.cassandra.lucene.index.RAMIndex;
import com.stratio.cassandra.lucene.key.PartitionMapper;
import com.stratio.cassandra.lucene.key.TokenMapper;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.search.SearchBuilder;
import com.stratio.cassandra.lucene.util.SimplePartitionIterator;
import com.stratio.cassandra.lucene.util.SimpleRowIterator;
import com.stratio.cassandra.lucene.util.TaskQueue;
import com.stratio.cassandra.lucene.util.TimeCounter;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.RowFilter.Expression;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.SortField.FIELD_SCORE;

/**
 * Lucene {@link Index} service provider.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class IndexService {

    protected static final Logger logger = LoggerFactory.getLogger(IndexService.class);

    protected final ColumnFamilyStore table;
    protected final CFMetaData metadata;
    protected final FSIndex lucene;
    protected final String name;
    protected final String qualifiedName;
    protected final TaskQueue queue;
    protected final Schema schema;
    protected final TokenMapper tokenMapper;
    protected final PartitionMapper partitionMapper;
    protected final ColumnsMapper columnsMapper;
    protected final boolean mapsMultiCells;

    private final SearchCache searchCache;

    /**
     * Constructor using the specified indexed table and index metadata.
     *
     * @param indexedTable the indexed table
     * @param indexMetadata the index metadata
     */
    protected IndexService(ColumnFamilyStore indexedTable, IndexMetadata indexMetadata) {
        table = indexedTable;
        metadata = table.metadata;

        // Setup monitoring names
        name = indexMetadata.name;
        qualifiedName = String.format("%s.%s.%s", metadata.ksName, metadata.cfName, indexMetadata.name);
        String mbeanName = String.format("com.stratio.cassandra.lucene:type=Lucene,keyspace=%s,table=%s,index=%s",
                                         metadata.ksName,
                                         metadata.cfName,
                                         name);

        // Setup cache, index and write queue
        IndexOptions options = new IndexOptions(metadata, indexMetadata);
        searchCache = new SearchCache(metadata, options.searchCacheSize);
        lucene = new FSIndex(name,
                             mbeanName,
                             options.path,
                             options.schema.getAnalyzer(),
                             options.refreshSeconds,
                             options.ramBufferMB,
                             options.maxMergeMB,
                             options.maxCachedMB,
                             searchCache::invalidate);
        queue = new TaskQueue(options.indexingThreads, options.indexingQueuesSize);

        // Setup mapping
        schema = options.schema;
        tokenMapper = new TokenMapper(options.tokenRangeCacheSize);
        partitionMapper = new PartitionMapper(metadata);
        columnsMapper = new ColumnsMapper();
        mapsMultiCells = metadata.allColumns()
                                 .stream()
                                 .filter(x -> schema.getMappedCells().contains(x.name.toString()))
                                 .anyMatch(x -> x.type.isMultiCell());
    }

    /**
     * Returns a new index service for the specified indexed table and index metadata.
     *
     * @param table the indexed table
     * @param indexMetadata the index metadata
     * @return the index service
     */
    public static IndexService build(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        return table.getComparator().subtypes().isEmpty()
               ? new IndexServiceSkinny(table, indexMetadata)
               : new IndexServiceWide(table, indexMetadata);
    }

    /**
     * Returns the names of the Lucene fields to be loaded from index during searches.
     *
     * @return the names of the fields to be loaded
     */
    public abstract Set<String> fieldsToLoad();

    /**
     * Returns the Lucene {@link SortField}s required to retrieve documents sorted by Cassandra's primary key.
     *
     * @return the sort fields
     */
    public abstract List<SortField> keySortFields();

    /**
     * Returns a {@link Columns} representing the specified {@link Row}.
     *
     * @param key the partition key
     * @param row the {@link Row}
     * @return the columns representing the specified {@link Row}
     */
    public abstract Columns columns(DecoratedKey key, Row row);

    /**
     * Returns a {@code Optional} with the Lucene {@link Document} representing the specified {@link Row}, or  an empty
     * {@code Optional} instance if the {@link Row} doesn't contain any of the columns mapped by the  {@link Schema}.
     *
     * @param key the partition key
     * @param row the {@link Row}
     * @return maybe a document
     */
    public abstract Optional<Document> document(DecoratedKey key, Row row);

    /**
     * Returns a Lucene {@link Term} uniquely identifying the specified {@link Row}.
     *
     * @param key the partition key
     * @param row the {@link Row}
     * @return a Lucene identifying {@link Term}
     */
    public abstract Term term(DecoratedKey key, Row row);

    /**
     * Returns a Lucene {@link Term} uniquely identifying the specified {@link Document}.
     *
     * @param document the document
     * @return a Lucene identifying {@link Term}
     */
    public abstract Term term(Document document);

    /**
     * Returns a Lucene {@link Term} identifying documents representing all the {@link Row}'s which are in the partition
     * the specified {@link DecoratedKey}.
     *
     * @param key the partition key
     * @return a Lucene {@link Term} representing {@code key}
     */
    public Term term(DecoratedKey key) {
        return partitionMapper.term(key);
    }

    /**
     * Returns if SSTables can contain additional columns of the specified {@link Row} so read-before-write is required
     * prior to indexing.
     *
     * @param key the partition key
     * @param row the {@link Row}
     * @return {@code true} if read-before-write is required, {@code false} otherwise
     */
    public boolean needsReadBeforeWrite(DecoratedKey key, Row row) {
        if (mapsMultiCells) {
            return true;
        } else {
            Columns columns = columns(key, row);
            return schema.getMappedCells().stream().anyMatch(x -> columns.getColumnsByCellName(x).isEmpty());
        }
    }

    /**
     * Returns a {@link NavigableSet} of the specified clusterings, sorted by the table metadata.
     *
     * @param clusterings the clusterings to be included in the set
     * @return the navigable sorted set
     */
    public NavigableSet<Clustering> clusterings(Clustering... clusterings) {
        NavigableSet<Clustering> sortedClusterings = new TreeSet<>(metadata.comparator);
        if (clusterings.length > 0) {
            sortedClusterings.addAll(Arrays.asList(clusterings));
        }
        return sortedClusterings;
    }

    /**
     * Returns the {@link DecoratedKey} contained in the specified Lucene {@link Document}.
     *
     * @param document the {@link Document} containing the partition key to be get
     * @return the {@link DecoratedKey} contained in the specified Lucene {@link Document}
     */
    public DecoratedKey decoratedKey(Document document) {
        return partitionMapper.decoratedKey(document);
    }

    /**
     * Creates an new {@code IndexWriter} object for updates to a given partition.
     *
     * @param key key of the partition being modified
     * @param nowInSec current time of the update operation
     * @param opGroup operation group spanning the update operation
     * @param transactionType what kind of update is being performed on the base data
     * @return the newly created {@code IndexWriter}
     */
    public abstract IndexWriter indexWriter(DecoratedKey key,
                                            int nowInSec,
                                            OpOrder.Group opGroup,
                                            IndexTransaction.Type transactionType);

    /** Commits the pending changes. */
    public final void commit() {
        queue.submitSynchronous(lucene::commit);
    }

    /** Deletes all the index contents. */
    public final void truncate() {
        queue.submitSynchronous(lucene::truncate);
    }

    /** Closes and removes all the index files. */
    public final void delete() {
        queue.shutdown();
        lucene.delete();
    }

    /**
     * Upserts the specified {@link Row}.
     *
     * @param key the partition key
     * @param row the row to be upserted
     */
    public void upsert(DecoratedKey key, Row row) {
        queue.submitAsynchronous(key, () -> document(key, row).ifPresent(document -> {
            Term term = term(key, row);
            lucene.upsert(term, document);
        }));
    }

    /**
     * Deletes the partition identified by the specified key.
     *
     * @param key the partition key
     * @param row the row to be deleted
     */
    public void delete(DecoratedKey key, Row row) {
        queue.submitAsynchronous(key, () -> {
            Term term = term(key, row);
            lucene.delete(term);
        });
    }

    /**
     * Deletes the partition identified by the specified key.
     *
     * @param key the partition key
     */
    public void delete(DecoratedKey key) {
        queue.submitAsynchronous(key, () -> {
            Term term = term(key);
            lucene.delete(term);
        });
    }

    /**
     * Returns a new {@link Index.Searcher} for the specified {@link ReadCommand}.
     *
     * @param command the read command being executed
     * @return a searcher with which to perform the supplied command
     */
    public Index.Searcher searcher(ReadCommand command) {

        // Parse search
        String expression = expression(command);
        Search search = SearchBuilder.fromJson(expression).build();
        Sort sort = sort(search);

        // Refresh if required
        if (search.refresh()) {
            lucene.refresh();
        }

        // Try luck with cache
        Optional<SearchCacheEntry> optional = searchCache.get(expression, command);
        if (optional.isPresent()) {
            logger.debug("Search cache hits");
            SearchCacheEntry entry = optional.get();
            Query query = entry.getQuery();
            ScoreDoc after = entry.getScoreDoc();
            SearchCacheUpdater cacheUpdater = entry.updater();
            return (ReadOrderGroup orderGroup) -> read(query, sort, after, command, orderGroup, cacheUpdater);
        } else {
            logger.debug("Search cache fails");
            Query query = new CachingWrapperQuery(query(search, command));
            searchCache.put(expression, command, query);
            SearchCacheUpdater cacheUpdater = searchCache.updater(expression, command, query);
            return (ReadOrderGroup orderGroup) -> read(query, sort, null, command, orderGroup, cacheUpdater);
        }
    }

    /**
     * Returns the the {@link Search} contained in the specified {@link ReadCommand}.
     *
     * @param command the read command containing the {@link Search}
     * @return the {@link Search} contained in {@code command}
     */
    public Search search(ReadCommand command) {
        return SearchBuilder.fromJson(expression(command)).build();
    }

    /**
     * Returns the the {@link Search} contained in the specified {@link ReadCommand}.
     *
     * @param command the read command containing the {@link Search}
     * @return the {@link Search} contained in {@code command}
     */
    public String expression(ReadCommand command) {
        for (Expression expression : command.rowFilter().getExpressions()) {
            if (expression.isCustom()) {
                RowFilter.CustomExpression customExpression = (RowFilter.CustomExpression) expression;
                if (name.equals(customExpression.getTargetIndex().name)) {
                    ByteBuffer bb = customExpression.getValue();
                    return UTF8Type.instance.compose(bb);
                }
            }
        }
        throw new IndexException("Lucene search expression not found in command expressions");
    }

    /**
     * Returns the Lucene {@link Query} represented by the specified {@link Search} and key filter.
     *
     * @param search the search
     * @param command the command
     * @return a Lucene {@link Query}
     */
    public Query query(Search search, ReadCommand command) {
        Query searchQuery = search.query(schema);
        Optional<Query> maybeKeyRangeQuery = query(command);
        if (maybeKeyRangeQuery.isPresent()) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(maybeKeyRangeQuery.get(), FILTER);
            builder.add(searchQuery, MUST);
            return builder.build();
        } else {
            return searchQuery;
        }
    }

    /**
     * Returns the key range query represented by the specified {@link ReadCommand}.
     *
     * @param command the read command
     * @return the key range query
     */
    private Optional<Query> query(ReadCommand command) {
        if (command instanceof SinglePartitionReadCommand) {
            DecoratedKey key = ((SinglePartitionReadCommand) command).partitionKey();
            ClusteringIndexFilter clusteringFilter = command.clusteringIndexFilter(key);
            return Optional.of(query(key, clusteringFilter));
        } else if (command instanceof PartitionRangeReadCommand) {
            DataRange dataRange = ((PartitionRangeReadCommand) command).dataRange();
            return query(dataRange);
        } else {
            throw new IndexException("Unsupported read command %s", command.getClass());
        }
    }

    /**
     * Returns a Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link DecoratedKey} and
     * {@link ClusteringIndexFilter}.
     *
     * @param key the partition key
     * @param filter the clustering key range
     * @return a query to get the {@link Document}s satisfying the key range
     */
    abstract Query query(DecoratedKey key, ClusteringIndexFilter filter);

    /**
     * Returns a Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link DataRange}.
     *
     * @param dataRange the {@link DataRange}
     * @return a query to get the {@link Document}s satisfying the {@code dataRange}
     */
    abstract Optional<Query> query(DataRange dataRange);

    /**
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified partition position.
     *
     * @param position the ring position
     * @return the query
     */
    public Query query(PartitionPosition position) {
        return position instanceof DecoratedKey
               ? partitionMapper.query((DecoratedKey) position)
               : tokenMapper.query(position.getToken());
    }

    /**
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified partition range.
     *
     * @param start the lower accepted partition position, {@code null} means no lower limit
     * @param stop the upper accepted partition position, {@code null} means no upper limit
     * @return the query, or {@code null} if it doesn't filter anything
     */
    public Optional<Query> query(PartitionPosition start, PartitionPosition stop) {
        return tokenMapper.query(start, stop);
    }

    /**
     * Returns the Lucene {@link Sort} with the specified {@link Search} sorting requirements followed by the
     * Cassandra's natural ordering based on partitioning token and cell name.
     *
     * @param search the {@link Search} containing sorting requirements
     * @return a Lucene sort according to {@code search}
     */
    private Sort sort(Search search) {
        List<SortField> sortFields = new ArrayList<>();
        if (search.usesSorting()) {
            sortFields.addAll(search.sortFields(schema));
        }
        if (search.usesRelevance()) {
            sortFields.add(FIELD_SCORE);
        }
        sortFields.addAll(keySortFields());
        return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
    }

    /**
     * Retrieves from the local storage the {@link Row}s in the specified partition slice.
     *
     * @param key the partition key
     * @param clusterings the clustering keys
     * @param nowInSec max allowed time in seconds
     * @param opGroup operation group spanning the calling operation
     * @return a {@link Row} iterator
     */
    public UnfilteredRowIterator read(DecoratedKey key,
                                      NavigableSet<Clustering> clusterings,
                                      int nowInSec,
                                      OpOrder.Group opGroup) {
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
        ColumnFilter columnFilter = ColumnFilter.all(metadata);
        return SinglePartitionReadCommand.create(metadata, nowInSec, key, columnFilter, filter)
                                         .queryMemtableAndDisk(table, opGroup);
    }

    /**
     * Retrieves from the local storage all the {@link Row}s in the specified partition.
     *
     * @param key the partition key
     * @param nowInSec max allowed time in seconds
     * @param opGroup operation group spanning the calling operation
     * @return a {@link Row} iterator
     */
    public UnfilteredRowIterator read(DecoratedKey key, int nowInSec, OpOrder.Group opGroup) {
        return read(key, clusterings(Clustering.EMPTY), nowInSec, opGroup);
    }

    /**
     * Reads from the local SSTables the rows identified by the specified search.
     *
     * @param query the Lucene query
     * @param sort the Lucene sort
     * @param after the last Lucene doc
     * @param command the Cassandra command
     * @param orderGroup the Cassandra read order group
     * @param cacheUpdater the search cache updater
     * @return the local {@link Row}s satisfying the search
     */
    public UnfilteredPartitionIterator read(Query query,
                                            Sort sort,
                                            ScoreDoc after,
                                            ReadCommand command,
                                            ReadOrderGroup orderGroup,
                                            SearchCacheUpdater cacheUpdater) {
        int limit = command.limits().count();
        DocumentIterator documents = lucene.search(query, sort, after, limit, fieldsToLoad());
        return indexReader(documents, command, orderGroup, cacheUpdater);
    }

    /**
     * Reads from the local SSTables the rows identified by the specified search.
     *
     * @param documents the Lucene documents
     * @param command the Cassandra command
     * @param orderGroup the Cassandra read order group
     * @param cacheUpdater the search cache updater
     * @return the local {@link Row}s satisfying the search
     */
    public abstract IndexReader indexReader(DocumentIterator documents,
                                            ReadCommand command,
                                            ReadOrderGroup orderGroup,
                                            SearchCacheUpdater cacheUpdater);

    /**
     * Post processes in the coordinator node the results of a distributed search. Gets the k globally best results from
     * all the k best node-local results.
     *
     * @param partitions the node results iterator
     * @param command the read command
     * @return the k globally best results
     */
    public PartitionIterator postProcess(PartitionIterator partitions, ReadCommand command) {

        Search search = search(command);

        // Skip if search does not require full scan
        if (search.requiresFullScan()) {

            List<Pair<DecoratedKey, SimpleRowIterator>> collectedRows = collect(partitions);

            Query query = search.query(schema);
            Sort sort = sort(search);
            int limit = command.limits().count();

            // Skip if search is not top-k TODO: Skip if only one partitioner range is involved
            if (search.isTopK()) {
                return process(query, sort, limit, collectedRows);
            }
        }
        return partitions;
    }

    private List<Pair<DecoratedKey, SimpleRowIterator>> collect(PartitionIterator partitions) {
        List<Pair<DecoratedKey, SimpleRowIterator>> rows = new ArrayList<>();
        TimeCounter time = TimeCounter.create().start();
        try {
            while (partitions.hasNext()) {
                try (RowIterator partition = partitions.next()) {
                    DecoratedKey key = partition.partitionKey();
                    while (partition.hasNext()) {
                        SimpleRowIterator rowIterator = new SimpleRowIterator(partition);
                        rows.add(Pair.create(key, rowIterator));
                    }
                }
            }
        } finally {
            logger.debug("Collected {} rows in {}", rows.size(), time.stop());
        }
        return rows;
    }

    private SimplePartitionIterator process(Query query,
                                            Sort sort,
                                            int limit,
                                            List<Pair<DecoratedKey, SimpleRowIterator>> collectedRows) {
        TimeCounter sortTime = TimeCounter.create().start();
        List<SimpleRowIterator> processedRows = new LinkedList<>();
        try {

            // Index collected rows in memory
            RAMIndex index = new RAMIndex(schema.getAnalyzer());
            Map<Term, SimpleRowIterator> rowsByTerm = new HashMap<>();
            for (Pair<DecoratedKey, SimpleRowIterator> pair : collectedRows) {
                DecoratedKey key = pair.left;
                SimpleRowIterator rowIterator = pair.right;
                Row row = rowIterator.getRow();
                Term term = term(key, row);
                Document document = document(key, row).get();
                rowsByTerm.put(term, rowIterator);
                index.add(document);
            }

            // Repeat search to sort partial results
            List<Document> documents = index.search(query, sort, limit, fieldsToLoad());
            index.close();

            // Collect post processed results
            for (Document document : documents) {
                Term term = term(document);
                SimpleRowIterator rowIterator = rowsByTerm.get(term);
                processedRows.add(rowIterator);
            }

        } finally {
            logger.debug("Post-processed {} collected rows to {} rows in {}",
                         collectedRows.size(),
                         processedRows.size(),
                         sortTime.stop());
        }
        return new SimplePartitionIterator(processedRows);
    }

    /**
     * Ensures that values present in the specified {@link PartitionUpdate} are valid according to the {@link Schema}.
     *
     * @param update the partition update containing the values to be validated
     */
    public void validate(PartitionUpdate update) {
        DecoratedKey key = update.partitionKey();
        for (Row row : update) {
            schema.validate(columns(key, row));
        }
    }
}
