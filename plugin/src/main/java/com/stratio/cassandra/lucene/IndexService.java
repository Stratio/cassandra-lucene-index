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
import com.stratio.cassandra.lucene.util.DecoratedPartition;
import com.stratio.cassandra.lucene.util.DecoratedRow;
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
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

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
    protected final Set<String> fieldsToLoad;
    protected final List<SortField> keySortFields;

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

        // Setup Lucene index and its write queue
        IndexOptions options = new IndexOptions(metadata, indexMetadata);
        lucene = new FSIndex(name,
                             mbeanName,
                             options.path,
                             options.schema.getAnalyzer(),
                             options.refreshSeconds,
                             options.ramBufferMB,
                             options.maxMergeMB,
                             options.maxCachedMB);
        queue = new TaskQueue(options.indexingThreads, options.indexingQueuesSize);

        // Setup mapping
        schema = options.schema;
        tokenMapper = new TokenMapper();
        partitionMapper = new PartitionMapper(metadata);
        columnsMapper = new ColumnsMapper();
        mapsMultiCells = metadata.allColumns()
                                 .stream()
                                 .filter(x -> schema.getMappedCells().contains(x.name.toString()))
                                 .anyMatch(x -> x.type.isMultiCell());

        // Setup fields to load
        fieldsToLoad = new HashSet<>();
        fieldsToLoad.add(PartitionMapper.FIELD_NAME);

        // Setup natural sort
        keySortFields = new ArrayList<>();
        keySortFields.add(tokenMapper.sortField());
        keySortFields.add(partitionMapper.sortField());
    }

    /**
     * Returns a new index service for the specified indexed table and index metadata.
     *
     * @param table the indexed table
     * @param indexMetadata the index metadata
     */
    public static IndexService build(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        return table.getComparator().subtypes().isEmpty()
               ? new IndexServiceSkinny(table, indexMetadata)
               : new IndexServiceWide(table, indexMetadata);
    }

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
        queue.submitAsynchronous(key, () ->
                document(key, row).ifPresent(document -> {
                    Term term = term(key, row);
                    lucene.upsert(term, document);
                })
        );
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
    public Index.Searcher searcherFor(ReadCommand command) {

        // Parse search data
        Search search = search(command);
        Query filter = query(command);
        Query query = search.query(schema, filter);
        Sort sort = sort(search);

        // Refresh if required
        if (search.refresh()) {
            lucene.refresh();
        }

        return (ReadExecutionController executionController) -> read(query, sort, null, command, executionController);
    }

    /**
     * Returns the the {@link Search} contained in the specified {@link ReadCommand}.
     *
     * @param command the read command containing the {@link Search}
     * @return the {@link Search} contained in {@code command}
     */
    private Search search(ReadCommand command) {
        for (Expression expression : command.rowFilter().getExpressions()) {
            if (expression.isCustom()) {
                RowFilter.CustomExpression customExpression = (RowFilter.CustomExpression) expression;
                if (name.equals(customExpression.getTargetIndex().name)) {
                    ByteBuffer bb = customExpression.getValue();
                    String json = UTF8Type.instance.compose(bb);
                    return SearchBuilder.fromJson(json).build();
                }
            }
        }
        throw new IndexException("Lucene search expression not found in command expressions");
    }

    /**
     * Returns the key range query represented by the specified {@link ReadCommand}.
     *
     * @param command the read command
     * @return the key range query
     */
    private Query query(ReadCommand command) {
        if (command instanceof SinglePartitionReadCommand) {
            DecoratedKey key = ((SinglePartitionReadCommand) command).partitionKey();
            ClusteringIndexFilter clusteringFilter = command.clusteringIndexFilter(key);
            return query(key, clusteringFilter);
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
     * @param clusteringFilter the clustering key range
     * @return a query to get the {@link Document}s satisfying the key range
     */
    abstract Query query(DecoratedKey key, ClusteringIndexFilter clusteringFilter);

    /**
     * Returns a Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link DataRange}.
     *
     * @param dataRange the {@link DataRange}
     * @return a query to get the {@link Document}s satisfying the {@code dataRange}
     */
    abstract Query query(DataRange dataRange);

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
        sortFields.addAll(keySortFields);
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
     * @param controller the Cassandra read execution controller
     * @return the local {@link Row}s satisfying the search
     */
    public UnfilteredPartitionIterator read(Query query,
                                            Sort sort,
                                            ScoreDoc after,
                                            ReadCommand command,
                                            ReadExecutionController controller) {
        int limit = command.limits().count();
        DocumentIterator documents = lucene.search(query, sort, after, limit, fieldsToLoad);
        return indexReader(documents, command, controller);
    }

    /**
     * Reads from the local SSTables the rows identified by the specified search.
     *
     * @param documents the Lucene documents
     * @param command the Cassandra command
     * @param controller the Cassandra read execution controller
     * @return the local {@link Row}s satisfying the search
     */
    public abstract IndexReader indexReader(DocumentIterator documents,
                                            ReadCommand command,
                                            ReadExecutionController controller);

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

        // TODO: Skip if only one node is involved
        // Skip if search is not top-k
        if (!search.isTopK()) {
            return partitions;
        }

        TimeCounter time = TimeCounter.create().start();

        Query query = search.query(schema);
        Sort sort = sort(search);
        int limit = command.limits().count();

        int count = 0;
        RAMIndex index = new RAMIndex(schema.getAnalyzer());
        Map<Term, DecoratedRow> rowsByTerm = new HashMap<>();
        while (partitions.hasNext()) {
            try (RowIterator partition = partitions.next()) {
                while (partition.hasNext()) {
                    DecoratedRow row = new DecoratedRow(partition);
                    Term term = term(row.partitionKey(), row.getRow());
                    Document document = document(partition.partitionKey(), row.getRow()).get();
                    rowsByTerm.put(term, row);
                    index.add(document);
                    count++;
                }
            }
        }
        List<Document> documents = index.search(query, sort, limit, fieldsToLoad);
        index.close();

        List<DecoratedRow> rows = new ArrayList<>(limit);
        for (Document document : documents) {
            Term term = term(document);
            DecoratedRow row = rowsByTerm.get(term);
            rows.add(row);
        }

        logger.debug("Post-processed {} rows in {}", count, time.stop());

        return new DecoratedPartition(rows);
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
