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
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.RowFilter.Expression;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.lucene.search.SortField.FIELD_SCORE;

/**
 * Class for providing operations between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class IndexService {

    protected static final Logger logger = LoggerFactory.getLogger(IndexService.class);

    protected final ColumnFamilyStore table;
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
     * Constructor using the specified {@link IndexOptions}.
     *
     * @param table The indexed table.
     * @param indexMetadata The index metadata.
     */
    protected IndexService(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        this.table = table;
        name = indexMetadata.name;
        qualifiedName = String.format("%s.%s.%s", table.metadata.ksName, table.metadata.cfName, indexMetadata.name);
        String mbean = String.format("com.stratio.cassandra.lucene:type=LuceneIndexes,keyspace=%s,table=%s,index=%s",
                                     table.metadata.ksName,
                                     table.metadata.cfName,
                                     name);

        // Setup index
        IndexOptions options = new IndexOptions(table.metadata, indexMetadata);
        lucene = new FSIndex(mbean,
                             name,
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
        partitionMapper = new PartitionMapper(table.metadata);
        columnsMapper = new ColumnsMapper(table.metadata);
        mapsMultiCells = table.metadata.allColumns()
                                       .stream()
                                       .filter(x -> schema.getMappedCells().contains(x.name.toString()))
                                       .anyMatch(x -> x.type.isMultiCell());

        // Setup fields to load
        fieldsToLoad = new HashSet<>();
        fieldsToLoad.add(PartitionMapper.FIELD_NAME);

        keySortFields = new ArrayList<>();
        keySortFields.add(tokenMapper.sortField());
        keySortFields.add(partitionMapper.sortField());
    }

    /**
     * Constructor using the specified {@link IndexOptions}.
     *
     * @param table The indexed table.
     * @param indexMetadata The index metadata.
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
     * @return the columns representing the specified {@link Row}.
     */
    public abstract Columns columns(DecoratedKey key, Row row);

    /**
     * Returns a {@code Optional} with the Lucene {@link Document} representing the specified {@link Row}, or  an empty
     * {@code Optional} instance if the {@link Row} doesn't contain any of the columns mapped by the  {@link Schema}.
     *
     * @param key the partition key
     * @param row the {@link Row}
     * @return a document
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
     * @return {@code true} if read-before-write is required, {@code false} otherwise.
     */
    public boolean needsReadBeforeWrite(DecoratedKey key, Row row) {
        if (mapsMultiCells) {
            return true;
        } else {
            Columns columns = columns(key, row);
            return schema.getMappedCells().stream().anyMatch(x -> columns.getColumnsByCellName(x).isEmpty());
        }
    }

    public NavigableSet<Clustering> clusterings(Clustering... clusterings) {
        NavigableSet<Clustering> sortedClusterings = new TreeSet<>(table.metadata.comparator);
        if (clusterings.length > 0) {
            sortedClusterings.addAll(Arrays.asList(clusterings));
        }
        return sortedClusterings;
    }

    /**
     * Creates an new {@code IndexWriter} object for updates to a given partition.
     *
     * @param key key of the partition being modified
     * @param nowInSec current time of the update operation
     * @param opGroup operation group spanning the update operation
     * @param transactionType indicates what kind of update is being performed on the base data i.e. a write time
     * insert/update/delete or the result of compaction
     * @return the newly created {@code IndexWriter}
     */
    public abstract IndexWriter indexWriter(DecoratedKey key,
                                            int nowInSec,
                                            OpOrder.Group opGroup,
                                            IndexTransaction.Type transactionType);

    /**
     * Commits the pending changes.
     */
    public final void commit() {
        queue.submitSynchronous(lucene::commit);
    }

    /**
     * Deletes all the index contents.
     */
    public final void truncate() {
        queue.submitSynchronous(lucene::truncate);
    }

    /**
     * Closes and removes all the index files.
     */
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
     * Factory method for query time search helper. Custom index implementations should perform any validation of query
     * expressions here and throw a meaningful InvalidRequestException when any expression is invalid.
     *
     * @param command the read command being executed
     * @return an Searcher with which to perform the supplied command supported by the index implementation.
     */
    public Index.Searcher searcherFor(ReadCommand command) {
        Search search = search(command);
        Filter filter = filter(command);
        Query query = search.query(schema, filter);
        Sort sort = sort(search);
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

    private Filter filter(ReadCommand command) {
        if (command instanceof SinglePartitionReadCommand) {
            return filter((SinglePartitionReadCommand) command);
        } else if (command instanceof PartitionRangeReadCommand) {
            return filter((PartitionRangeReadCommand) command);
        } else {
            throw new IndexException("Unsupported read command %s", command.getClass());
        }
    }

    private Filter filter(SinglePartitionReadCommand command) {
        DecoratedKey key = command.partitionKey();
        return new QueryWrapperFilter(new TermQuery(term(key)));
    }

    private Filter filter(PartitionRangeReadCommand command) {
        return null;
    }

    /**
     * Returns the Lucene {@link Sort} with the specified {@link Search} sorting requirements followed by the
     * Cassandra's natural ordering based on partitioning token and cell name.
     *
     * @param search the {@link Search} containing sorting requirements
     * @return a Lucene {@link Sort}
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
     * Reads from the local SSTables the rows identified by the specified search.
     *
     * @param query the Lucene query
     * @param sort the Lucene sort
     * @param after the last Lucene doc
     * @param command the Cassandra command
     * @param executionController the Cassandra read execution controller
     * @return the local {@link Row}s satisfying the search
     */
    public abstract UnfilteredPartitionIterator read(Query query,
                                                     Sort sort,
                                                     ScoreDoc after,
                                                     ReadCommand command,
                                                     ReadExecutionController executionController);

    /**
     * Post processes in the coordinator node the results of a distributed search. Gets the k globally best results from
     * all the k best node-local results.
     *
     * @param partitions the node results iterator
     * @param command the read command
     * @return the globally best results
     */
    public PartitionIterator postProcess(PartitionIterator partitions, ReadCommand command) {

        // TODO: Skip if search is not top-k
        // TODO: Skip if only one node is involved

        Search search = search(command);
        Query query = search.query(schema);
        Sort sort = sort(search);
        int limit = command.limits().count();

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
        return new DecoratedPartition(rows);
    }
}
