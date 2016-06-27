/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.stratio.cassandra.lucene.util.*;
import com.stratio.cassandra.lucene.util.TaskQueue;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.RowFilter.Expression;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.lucene.search.SortField.FIELD_SCORE;

/**
 * Lucene {@link Index} service provider.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
abstract class IndexService implements IndexServiceMBean {

    protected static final Logger logger = LoggerFactory.getLogger(IndexService.class);

    final String qualifiedName;
    final TokenMapper tokenMapper;
    final PartitionMapper partitionMapper;
    final ColumnsMapper columnsMapper;
    protected final ColumnFamilyStore table;
    protected final CFMetaData metadata;
    protected final Schema schema;
    private final FSIndex lucene;
    private final String name;
    private final String column;
    private final ColumnDefinition columnDefinition;
    private final TaskQueue queue;
    private final boolean mapsMultiCells;
    private String mbeanName;
    private ObjectName mbean;

    /**
     * Constructor using the specified indexed table and index metadata.
     *
     * @param indexedTable the indexed table
     * @param indexMetadata the index metadata
     */
    IndexService(ColumnFamilyStore indexedTable, IndexMetadata indexMetadata) {

        table = indexedTable;
        metadata = table.metadata;
        name = indexMetadata.name;
        column = column(indexMetadata);
        columnDefinition = columnDefinition(metadata, column);
        qualifiedName = String.format("%s.%s.%s", metadata.ksName, metadata.cfName, indexMetadata.name);
        mbeanName = String.format("com.stratio.cassandra.lucene:type=Lucene,keyspace=%s,table=%s,index=%s",
                                  metadata.ksName,
                                  metadata.cfName,
                                  name);

        // Parse options
        IndexOptions options = new IndexOptions(metadata, indexMetadata);

        // Setup mapping
        schema = options.schema;
        tokenMapper = new TokenMapper();
        partitionMapper = new PartitionMapper(metadata);
        columnsMapper = new ColumnsMapper();
        mapsMultiCells = metadata.allColumns()
                                 .stream()
                                 .filter(x -> schema.getMappedCells().contains(x.name.toString()))
                                 .anyMatch(x -> x.type.isMultiCell());

        // Setup FS index and write queue
        queue = new TaskQueue(options.indexingThreads, options.indexingQueuesSize);
        lucene = new FSIndex(name,
                             options.path,
                             options.schema.getAnalyzer(),
                             options.refreshSeconds,
                             options.ramBufferMB,
                             options.maxMergeMB,
                             options.maxCachedMB);
    }

    private static String column(IndexMetadata indexMetadata) {
        String column = indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME);
        return StringUtils.isBlank(column) ? null : column;
    }

    private static ColumnDefinition columnDefinition(CFMetaData metadata, String name) {
        if (StringUtils.isNotBlank(name)) {
            for (ColumnDefinition def : metadata.allColumns()) {
                if (def.name.toString().equals(name)) {
                    return def;
                }
            }
        }
        return null;
    }

    void init() {

        // Initialize index
        List<SortField> keySortFields = keySortFields();
        Sort keySort = new Sort(keySortFields.toArray(new SortField[keySortFields.size()]));
        try {
            lucene.init(keySort, fieldsToLoad());
        } catch (Exception e) {
            logger.error(String.format(
                    "Initialization of Lucene FS directory for index '%s' has failed, " +
                    "this could be caused by on-disk data corruption, " +
                    "or by an upgrade to an incompatible version, " +
                    "try to drop the failing index and create it again:",
                    name), e);
        }

        // Register JMX MBean
        try {
            mbean = new ObjectName(mbeanName);
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, this.mbean);
        } catch (JMException e) {
            logger.error("Error while registering Lucene index JMX MBean", e);
        }
    }

    /**
     * Returns a new index service for the specified indexed table and index metadata.
     *
     * @param table the indexed table
     * @param indexMetadata the index metadata
     * @return the index service
     */
    static IndexService build(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        return table.getComparator().subtypes().isEmpty()
               ? new IndexServiceSkinny(table, indexMetadata)
               : new IndexServiceWide(table, indexMetadata);
    }

    /**
     * Returns if the specified column definition is mapped by this index.
     *
     * @param columnDef a column definition
     * @return {@code true} if the column is mapped, {@code false} otherwise
     */
    boolean dependsOn(ColumnDefinition columnDef) {
        return schema.maps(columnDef);
    }

    /**
     * Returns if the specified {@link Expression} is targeted to this index
     *
     * @param expression a CQL query expression
     * @return {@code true} if {@code expression} is targeted to this index, {@code false} otherwise
     */
    private boolean supportsExpression(Expression expression) {
        return supportsExpression(expression.column(), expression.operator());
    }

    /**
     * Returns if a CQL expression with the specified {@link ColumnDefinition} and {@link Operator} is targeted to this
     * index
     *
     * @param columnDef the expression column definition
     * @param operator the expression operator
     * @return {@code true} if the expression is targeted to this index, {@code false} otherwise
     */
    boolean supportsExpression(ColumnDefinition columnDef, Operator operator) {
        return column != null &&
               operator == Operator.EQ &&
               column.equals(columnDef.name.toString()) &&
               columnDef.cellValueType() instanceof UTF8Type;
    }

    /**
     * Returns a copy of the specified {@link RowFilter} without any Lucene {@link Expression}s.
     *
     * @param filter a row filter
     * @return a copy of {@code filter} without Lucene {@link Expression}s
     */
    RowFilter getPostIndexQueryFilter(RowFilter filter) {
        if (column != null) {
            for (Expression expression : filter) {
                if (supportsExpression(expression)) {
                    filter = filter.without(expression);
                }
            }
        }
        return filter;
    }

    /**
     * Returns the validated {@link Search} contained in the specified expression.
     *
     * @param expression a custom CQL expression
     * @return the validated expression
     */
    Search validate(RowFilter.Expression expression) {
        ByteBuffer value = expression instanceof RowFilter.CustomExpression
                           ? ((RowFilter.CustomExpression) expression).getValue()
                           : expression.getIndexValue();
        String json = UTF8Type.instance.compose(value);
        Search search = SearchBuilder.fromJson(json).build();
        search.validate(schema);
        return search;
    }

    /**
     * Returns the names of the Lucene fields to be loaded from index during searches.
     *
     * @return the names of the fields to be loaded
     */
    abstract Set<String> fieldsToLoad();

    /**
     * Returns the Lucene {@link SortField}s required to retrieve documents sorted by Cassandra's primary key.
     *
     * @return the sort fields
     */
    abstract List<SortField> keySortFields();

    /**
     * Returns a {@link Columns} representing the specified {@link Row}.
     *
     * @param key the partition key
     * @param row the {@link Row}
     * @return the columns representing the specified {@link Row}
     */
    abstract Columns columns(DecoratedKey key, Row row);

    /**
     * Returns the Lucene {@link Document} representing the specified {@link Row}.
     *
     * Only the fields required by the post processing phase of the specified {@link Search} will be added.
     *
     * @param key the partition key
     * @param row the {@link Row}
     * @param search a search
     * @return a document
     */
    private Document document(DecoratedKey key, Row row, Search search) {
        Document document = new Document();
        Columns columns = columns(key, row);
        addKeyFields(document, key, row);
        schema.addPostProcessingFields(document, columns, search);
        return document;
    }

    protected abstract void addKeyFields(Document document, DecoratedKey key, Row row);

    /**
     * Returns a Lucene {@link Term} uniquely identifying the specified {@link Row}.
     *
     * @param key the partition key
     * @param row the {@link Row}
     * @return a Lucene identifying {@link Term}
     */
    abstract Term term(DecoratedKey key, Row row);

    /**
     * Returns a Lucene {@link Term} uniquely identifying the specified {@link Document}.
     *
     * @param document the document
     * @return a Lucene identifying {@link Term}
     */
    abstract Term term(Document document);

    /**
     * Returns a Lucene {@link Term} identifying documents representing all the {@link Row}'s which are in the partition
     * the specified {@link DecoratedKey}.
     *
     * @param key the partition key
     * @return a Lucene {@link Term} representing {@code key}
     */
    Term term(DecoratedKey key) {
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
    boolean needsReadBeforeWrite(DecoratedKey key, Row row) {
        if (mapsMultiCells) {
            return true;
        } else {
            Columns columns = columns(key, row);
            return schema.getMappedCells().stream().anyMatch(x -> columns.getByCellName(x).isEmpty());
        }
    }

    /**
     * Returns a {@link NavigableSet} of the specified clusterings, sorted by the table metadata.
     *
     * @param clusterings the clusterings to be included in the set
     * @return the navigable sorted set
     */
    NavigableSet<Clustering> clusterings(Clustering... clusterings) {
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
    DecoratedKey decoratedKey(Document document) {
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
    abstract IndexWriter indexWriter(DecoratedKey key,
                                     int nowInSec,
                                     OpOrder.Group opGroup,
                                     IndexTransaction.Type transactionType);

    /** Deletes all the index contents. */
    final void truncate() {
        queue.submitSynchronous(lucene::truncate);
    }

    /** Closes and removes all the index files. */
    final void delete() {
        try {
            queue.shutdown();
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(mbean);
        } catch (JMException e) {
            logger.error("Error while unregistering Lucene index MBean", e);
        } finally {
            lucene.delete();
        }
    }

    /**
     * Upserts the specified {@link Row}.
     *
     * @param key the partition key
     * @param row the row to be upserted
     * @param nowInSec now in seconds
     */
    void upsert(DecoratedKey key, Row row, int nowInSec) {
        queue.submitAsynchronous(key, () -> {
            Term term = term(key, row);
            Columns columns = columns(key, row).cleanDeleted(nowInSec);
            Document document = new Document();
            schema.addFields(document, columns);
            if (document.getFields().isEmpty()) {
                lucene.delete(term);
            } else {
                addKeyFields(document, key, row);
                lucene.upsert(term, document);
            }
        });
    }

    /**
     * Deletes the partition identified by the specified key.
     *
     * @param key the partition key
     * @param row the row to be deleted
     */
    void delete(DecoratedKey key, Row row) {
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
    void delete(DecoratedKey key) {
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
    Index.Searcher searcher(ReadCommand command) {

        // Parse search
        Tracer.trace("Building Lucene search");
        String expression = expression(command);
        Search search = SearchBuilder.fromJson(expression).build();
        Query query = search.query(schema, query(command).orElse(null));
        Query after = after(search.paging(), command);
        Sort sort = sort(search);
        int count = command.limits().count();

        // Refresh if required
        if (search.refresh()) {
            Tracer.trace("Refreshing Lucene index searcher");
            refresh();
        }

        // Search
        Tracer.trace("Lucene index searching for {} rows", count);
        DocumentIterator documents = lucene.search(after, query, sort, count);
        return (ReadOrderGroup orderGroup) -> indexReader(documents, command, orderGroup);
    }

    private Search search(ReadCommand command) {
        return SearchBuilder.fromJson(expression(command)).build();
    }

    private Search search(SinglePartitionReadCommand.Group group) {
        return SearchBuilder.fromJson(expression(group)).build();
    }

    private String expression(ReadCommand command) {
        for (Expression expression : command.rowFilter().getExpressions()) {
            if (expression.isCustom()) {
                RowFilter.CustomExpression customExpression = (RowFilter.CustomExpression) expression;
                if (name.equals(customExpression.getTargetIndex().name)) {
                    ByteBuffer bb = customExpression.getValue();
                    return UTF8Type.instance.compose(bb);
                }
            }
            if (supportsExpression(expression)) {
                ByteBuffer bb = expression.getIndexValue();
                return UTF8Type.instance.compose(bb);
            }
        }
        throw new IndexException("Lucene search expression not found in command expressions");
    }

    private String expression(SinglePartitionReadCommand.Group group) {
        String result = null;
        for (ReadCommand command : group.commands) {
            String expression = expression(command);
            if (result == null) {
                result = expression;
            } else if (!result.equals(expression)) {
                throw new IndexException("Unable to process command group with different index clauses");
            }
        }
        return result;
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
            throw new IndexException("Unsupported read command {}", command.getClass());
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
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified partition range.
     *
     * @param start the lower accepted partition position, {@code null} means no lower limit
     * @param stop the upper accepted partition position, {@code null} means no upper limit
     * @return the query to retrieve all the rows in the specified range
     */
    Optional<Query> query(PartitionPosition start, PartitionPosition stop) {
        return tokenMapper.query(start, stop);
    }

    private Query after(IndexPagingState pagingState, ReadCommand command) {
        try {
            if (pagingState != null) {
                Pair<DecoratedKey, Clustering> position = pagingState.forCommand(command);
                return position == null ? null : after(position.left, position.right).orElse(null);
            }
            return null;
        } catch (RuntimeException e) {
            throw new IndexException(e, "Invalid paging state");
        }
    }

    /**
     * Returns a Lucene {@link Query} to retrieve the row identified by the specified paging state.
     *
     * @param key the partition key
     * @param clustering the clustering key
     * @return the query to retrieve the row
     */
    abstract Optional<Query> after(DecoratedKey key, Clustering clustering);

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
    UnfilteredRowIterator read(DecoratedKey key,
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
    UnfilteredRowIterator read(DecoratedKey key, int nowInSec, OpOrder.Group opGroup) {
        return read(key, clusterings(Clustering.EMPTY), nowInSec, opGroup);
    }

    /**
     * Reads from the local SSTables the rows identified by the specified search.
     *
     * @param documents the Lucene documents
     * @param command the Cassandra command
     * @param orderGroup the Cassandra read order group
     * @return the local {@link Row}s satisfying the search
     */
    abstract IndexReader indexReader(DocumentIterator documents, ReadCommand command, ReadOrderGroup orderGroup);

    /**
     * Post processes in the coordinator node the results of a distributed search. Gets the k globally best results from
     * all the k best node-local results.
     *
     * @param partitions the node results iterator
     * @param group the read command group
     * @return the k globally best results
     */
    PartitionIterator postProcess(PartitionIterator partitions, SinglePartitionReadCommand.Group group) {

        // Skip unneeded post processing if only one partition is involved
        if (group.commands.size() <= 1) {
            return partitions;
        }

        Search search = search(group);
        int limit = group.limits().count();
        int nowInSec = group.nowInSec();
        return postProcess(partitions, search, limit, nowInSec);
    }

    /**
     * Post processes in the coordinator node the results of a distributed search. Gets the k globally best results from
     * all the k best node-local results.
     *
     * @param partitions the node results iterator
     * @param command the read command
     * @return the k globally best results
     */
    PartitionIterator postProcess(PartitionIterator partitions, ReadCommand command) {

        // Skip unneeded post processing if only one partition is involved
        if (command instanceof SinglePartitionReadCommand) {
            return partitions;
        }

        Search search = search(command);
        int limit = command.limits().count();
        int nowInSec = command.nowInSec();
        return postProcess(partitions, search, limit, nowInSec);
    }

    private PartitionIterator postProcess(PartitionIterator partitions, Search search, int limit, int nowInSec) {
        if (search.requiresFullScan()) {

            List<Pair<DecoratedKey, SimpleRowIterator>> collectedRows = collect(partitions);

            // Skip if the search doesn't require any kind of sorting
            if (search.requiresPostProcessing() && !collectedRows.isEmpty()) {
                return process(search, limit, nowInSec, collectedRows);
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

    private SimplePartitionIterator process(Search search,
                                            int limit,
                                            int nowInSec,
                                            List<Pair<DecoratedKey, SimpleRowIterator>> collectedRows) {
        TimeCounter time = TimeCounter.create().start();
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
                rowsByTerm.put(term, rowIterator);
                Document document = document(key, row, search);
                index.add(document);
            }

            // Repeat search to sort partial results
            Query query = search.postProcessingQuery(schema);
            Sort sort = sort(search);
            List<Pair<Document, ScoreDoc>> documents = index.search(query, sort, limit, fieldsToLoad());
            index.close();

            // Collect post processed results
            for (Pair<Document, ScoreDoc> pair : documents) {
                Document document = pair.left;
                Float score = pair.right.score;
                Term term = term(document);
                SimpleRowIterator rowIterator = rowsByTerm.get(term);
                rowIterator.setDecorator(row -> decorate(row, score, nowInSec));
                processedRows.add(rowIterator);
            }

        } finally {
            Tracer.trace("Lucene post-process {} collected rows to {} result rows",
                         collectedRows.size(),
                         processedRows.size());
            logger.debug("Post-processed {} collected rows to {} result rows in {}",
                         collectedRows.size(),
                         processedRows.size(),
                         time.stop());
        }
        return new SimplePartitionIterator(processedRows);
    }

    private Row decorate(Row row, Float score, int nowInSec) {
        if (column == null || score == null) {
            return row;
        }
        long timestamp = row.primaryKeyLivenessInfo().timestamp();
        Row.Builder builder = BTreeRow.unsortedBuilder(nowInSec);
        builder.newRow(row.clustering());
        builder.addRowDeletion(row.deletion());
        builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
        row.cells().forEach(builder::addCell);
        ByteBuffer value = UTF8Type.instance.decompose(Float.toString(score));
        builder.addCell(BufferCell.live(metadata, columnDefinition, timestamp, value));
        return builder.build();
    }

    /**
     * Ensures that values present in the specified {@link PartitionUpdate} are valid according to the {@link Schema}.
     *
     * @param update the partition update containing the values to be validated
     */
    void validate(PartitionUpdate update) {
        DecoratedKey key = update.partitionKey();
        for (Row row : update) {
            schema.validate(columns(key, row));
        }
    }

    /** {@inheritDoc} */
    @Override
    public final void commit() {
        queue.submitSynchronous(lucene::commit);
    }

    /** {@inheritDoc} */
    @Override
    public int getNumDocs() {
        return lucene.getNumDocs();
    }

    /** {@inheritDoc} */
    @Override
    public int getNumDeletedDocs() {
        return lucene.getNumDeletedDocs();
    }

    /** {@inheritDoc} */
    @Override
    public void forceMerge(int maxNumSegments, boolean doWait) {
        queue.submitSynchronous(() -> lucene.forceMerge(maxNumSegments, doWait));
    }

    /** {@inheritDoc} */
    @Override
    public void forceMergeDeletes(boolean doWait) {
        queue.submitSynchronous(() -> lucene.forceMergeDeletes(doWait));
    }

    /** {@inheritDoc} */
    @Override
    public void refresh() {
        queue.submitSynchronous(lucene::refresh);
    }
}
