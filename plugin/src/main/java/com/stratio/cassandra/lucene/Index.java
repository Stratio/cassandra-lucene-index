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

import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.search.SearchBuilder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

/**
 * {@link org.apache.cassandra.index.Index} that uses Apache Lucene as backend. It allows, among others, multi-column
 * and full-text search.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Index implements org.apache.cassandra.index.Index {

    private static final Logger logger = LoggerFactory.getLogger(Index.class);

    private final ColumnFamilyStore table;
    private final IndexMetadata indexMetadata;
    private IndexService service;
    private String name;

    // Setup CQL query handler
    static {
        try {
            Field field = ClientState.class.getDeclaredField("cqlQueryHandler");
            field.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(null, new IndexQueryHandler());
        } catch (Exception e) {
            logger.error("Unable to set Lucene CQL query handler", e);
        }
    }

    /**
     * Builds a new Lucene index for the specified {@link ColumnFamilyStore} using the specified {@link IndexMetadata}.
     *
     * @param table the indexed {@link ColumnFamilyStore}
     * @param indexMetadata the index's metadata
     */
    public Index(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        logger.debug("Building Lucene index {} {}", table.metadata, indexMetadata);
        this.table = table;
        this.indexMetadata = indexMetadata;
        try {
            service = IndexService.build(table, indexMetadata);
        } catch (Exception e) {
            throw new IndexException(e);
        }
        name = service.qualifiedName;
    }

    /**
     * Validates the specified index options.
     *
     * @param options the options to be validated
     * @param metadata the metadata of the table to be indexed
     * @return the validated options
     * @throws ConfigurationException if the options are not valid
     */
    public static Map<String, String> validateOptions(Map<String, String> options, CFMetaData metadata) {
        logger.debug("Validating Lucene index options");
        try {
            IndexOptions.validateOptions(options, metadata);
        } catch (IndexException e) {
            logger.error("Lucene index options are invalid", e);
            throw new ConfigurationException(e.getMessage());
        }
        logger.debug("Lucene index options are valid");
        return Collections.emptyMap();
    }

    /*
     * Management functions
     */

    /**
     * Return a task to perform any initialization work when a new index instance is created. This may involve costly
     * operations such as (re)building the index, and is performed asynchronously by SecondaryIndexManager
     *
     * @return a task to perform any necessary initialization work
     */
    @Override
    public Callable<?> getInitializationTask() {
        logger.info("Getting initialization task of {}", name);
        if (table.isEmpty() || SystemKeyspace.isIndexBuilt(table.keyspace.getName(), indexMetadata.name)) {
            logger.info("Index {} doesn't need (re)building", name);
            return null;
        } else {
            logger.info("Index {} needs (re)building", name);
            return () -> {
                table.forceBlockingFlush();
                service.truncate();
                table.indexManager.buildIndexBlocking(this);
                return null;
            };
        }
    }

    /**
     * Returns the IndexMetadata which configures and defines the index instance. This should be the same object passed
     * as the argument to setIndexMetadata.
     *
     * @return the index's metadata
     */
    @Override
    public IndexMetadata getIndexMetadata() {
        return indexMetadata;
    }

    /**
     * Return a task to reload the internal metadata of an index. Called when the base table metadata is modified or
     * when the configuration of the Index is updated Implementations should return a task which performs any necessary
     * work to be done due to updating the configuration(s) such as (re)building etc. This task is performed
     * asynchronously by SecondaryIndexManager
     *
     * @return task to be executed by the index manager during a reload
     */
    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata) { // TODO: Check rebuild
        return () -> {
            logger.debug("Reloading Lucene index {} metadata: {}", name, indexMetadata);
            return null;
        };
    }

    /**
     * An index must be registered in order to be able to either subscribe to update events on the base table and/or to
     * provide IndexSearcher functionality for reads. The double dispatch involved here, where the Index actually
     * performs its own registration by calling back to the supplied IndexRegistry's own registerIndex method, is to
     * make the decision as to whether or not to register an index belong to the implementation, not the manager.
     *
     * @param registry the index registry to register the instance with
     */
    @Override
    public void register(IndexRegistry registry) {
        registry.registerIndex(this);
    }

    /**
     * If the index implementation uses a local table to store its index data this method should return a handle to it.
     * If not, an empty Optional should be returned. Typically, this is useful for the built-in Index implementations.
     *
     * @return an Optional referencing the Index's backing storage table if it has one, or Optional.empty() if not
     */
    public Optional<ColumnFamilyStore> getBackingTable() {
        return Optional.empty();
    }

    /**
     * Return a task which performs a blocking flush of the index's data to persistent storage.
     *
     * @return task to be executed by the index manager to perform the flush
     */
    @Override
    public Callable<?> getBlockingFlushTask() {
        return () -> {
            logger.info("Flushing Lucene index {}", name);
            service.commit();
            return null;
        };
    }

    /**
     * Return a task which invalidates the index, indicating it should no longer be considered usable. This should
     * include an clean up and releasing of resources required when dropping an index.
     *
     * @return task to be executed by the index manager to invalidate the index
     */
    @Override
    public Callable<?> getInvalidateTask() {
        return () -> {
            service.delete();
            return null;
        };
    }

    /**
     * Return a task to truncate the index with the specified truncation timestamp. Called when the base table is
     * truncated.
     *
     * @param truncatedAt timestamp of the truncation operation. This will be the same timestamp used in the truncation
     * of the base table.
     * @return task to be executed by the index manager when the base table is truncated.
     */
    @Override
    public Callable<?> getTruncateTask(long truncatedAt) {
        logger.trace("Getting truncate task");
        return () -> {
            logger.info("Truncating Lucene index {}", name);
            service.truncate();
            logger.info("Truncated Lucene index {}", name);
            return null;
        };
    }

    /**
     * Return true if this index can be built or rebuilt when the index manager determines it is necessary. Returning
     * false enables the index implementation (or some other component) to control if and when SSTable data is
     * incorporated into the index.
     *
     * This is called by SecondaryIndexManager in buildIndexBlocking, buildAllIndexesBlocking and rebuildIndexesBlocking
     * where a return value of false causes the index to be excluded from the set of those which will process the
     * SSTable data.
     *
     * @return if the index should be included in the set which processes SSTable data, false otherwise.
     */
    @Override
    public boolean shouldBuildBlocking() {
        logger.trace("Asking if it should build blocking");
        return true;
    }

    /*
     * Index selection
     */

    /**
     * Called to determine whether this index targets a specific column. Used during schema operations such as when
     * dropping or renaming a column, to check if the index will be affected by the change. Typically, if an index
     * answers that it does depend upon a column, then schema operations on that column are not permitted until the
     * index is dropped or altered.
     *
     * @param column the column definition to check
     * @return true if the index depends on the supplied column being present; false if the column may be safely dropped
     * or modified without adversely affecting the index
     */
    @Override
    public boolean dependsOn(ColumnDefinition column) { // TODO: Could return true only for key and/or mapped columns
        logger.trace("Asking if it depends on column {}", column);
        return service.schema.maps(column);
    }

    /**
     * Called to determine whether this index can provide a searcher to execute a query on the supplied column using the
     * specified operator. This forms part of the query validation done before a CQL select statement is executed.
     *
     * @param column the target column of a search query predicate
     * @param operator the operator of a search query predicate
     * @return true if this index is capable of supporting such expressions, false otherwise
     */
    @Override
    public boolean supportsExpression(ColumnDefinition column, Operator operator) {
        logger.trace("Asking if it supports the expression {} {}", column, operator);
        return false;
    }

    /**
     * If the index supports custom search expressions using the {@code}SELECT * FROM table WHERE expr(index_name,
     * expression){@code} syntax, this method should return the expected type of the expression argument. For example,
     * if the index supports custom expressions as Strings, calls to this method should return
     * {@code}UTF8Type.instance{@code}. If the index implementation does not support custom expressions, then it should
     * return null.
     *
     * @return an the type of custom index expressions supported by this index, or an null if custom expressions are not
     * supported.
     */
    @Override
    public AbstractType<?> customExpressionValueType() {
        logger.trace("Requesting the custom expressions value type");
        return UTF8Type.instance;
    }

    /**
     * Transform an initial RowFilter into the filter that will still need to applied to a set of Rows after the index
     * has performed it's initial scan. Used in ReadCommand#executeLocal to reduce the amount of filtering performed on
     * the results of the index query.
     *
     * @param filter the initial filter belonging to a ReadCommand
     * @return the (hopefully) reduced filter that would still need to be applied after the index was used to narrow the
     * initial result set
     */
    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter) {
        logger.trace("Getting the post index query filter for {}", filter);
        return filter;
    }

    /**
     * Return an estimate of the number of results this index is expected to return for any given query that it can be
     * used to answer. Used in conjunction with indexes() and supportsExpression() to determine the most selective index
     * for a given ReadCommand. Additionally, this is also used by StorageProxy.estimateResultsPerRange to calculate the
     * initial concurrency factor for range requests
     *
     * @return the estimated average number of results aIndexSearcher may return for any given query
     */
    @Override
    public long getEstimatedResultRows() {
        logger.trace("Getting the estimated result rows");
        return 1;
    }

    /*
     * Input validation
     */

    /**
     * Called at write time to ensure that values present in the update are valid according to the rules of all
     * registered indexes which will process it. The partition key as well as the clustering and cell values for each
     * row in the update may be checked by index implementations
     *
     * @param update PartitionUpdate containing the values to be validated by registered Index implementations.
     * @throws InvalidRequestException If the update doesn't pass through the validation.
     */
    @Override
    public void validate(PartitionUpdate update) {
        logger.trace("Validating {}", update);
        try {
            service.validate(update);
        } catch (Exception e) {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    /*
     * Update processing
     */

    /**
     * Creates an new {@code IndexWriter} object for updates to a given partition.
     *
     * @param key key of the partition being modified
     * @param columns the regular and static columns the created indexer will have to deal with. This can be empty as an
     * update might only contain partition, range and row deletions, but the indexer is guaranteed to not get any cells
     * for a column that is not part of {@code columns}.
     * @param nowInSec current time of the update operation
     * @param opGroup operation group spanning the update operation
     * @param transactionType indicates what kind of update is being performed on the base data i.e. a write time
     * insert/update/delete or the result of compaction
     * @return the newly created indexer or {@code null} if the index is not interested by the update (this could be
     * because the index doesn't care about that particular partition, doesn't care about that type of transaction,
     * ...).
     */
    @Override
    public Indexer indexerFor(DecoratedKey key,
                              PartitionColumns columns,
                              int nowInSec,
                              OpOrder.Group opGroup,
                              IndexTransaction.Type transactionType) {
        return service.indexWriter(key, nowInSec, opGroup, transactionType);
    }

    /*
     * Querying
     */

    /**
     * Return a function which performs post processing on the results of a partition range read command. In future,
     * this may be used as a generalized mechanism for transforming results on the coordinator prior to returning them
     * to the caller.
     *
     * This is used on the coordinator during execution of a range command to perform post processing of merged results
     * obtained from the necessary replicas. This is the only way in which results are transformed in this way but this
     * may change over time as usage is generalized. See CASSANDRA-8717 for further discussion.
     *
     * The function takes a PartitionIterator of the results from the replicas which has already been collated and
     * reconciled, along with the command being executed. It returns another PartitionIterator containing the results of
     * the transformation (which may be the same as the input if the transformation is a no-op).
     */
    @Override
    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command) {
        return (partitions, readCommand) -> service.postProcess(partitions, readCommand);
    }

    /**
     * Factory method for query time search helper. Custom index implementations should perform any validation of query
     * expressions here and throw a meaningful InvalidRequestException when any expression is invalid.
     *
     * @param command the read command being executed
     * @return an IndexSearcher with which to perform the supplied command
     * @throws InvalidRequestException if the command's expressions are invalid according to the specific syntax
     * supported by the index implementation.
     */
    @Override
    public Searcher searcherFor(ReadCommand command) {
        logger.trace("Getting searcher for {}", command);
        try {
            return service.searcher(command);
        } catch (Exception e) {
            logger.error("Error while searching", e);
            throw new InvalidRequestException(e.getMessage());
        }
    }

    /**
     * Validates the specified {@link RowFilter.CustomExpression}.
     *
     * @param expression the expression to be validated
     * @return the valid search represented by {@code expression}
     * @throws InvalidRequestException if the expression is not valid
     */
    public Search validate(RowFilter.CustomExpression expression) {
        try {
            String json = UTF8Type.instance.compose(expression.getValue());
            Search search = SearchBuilder.fromJson(json).build();
            search.query(service.schema);
            return search;
        } catch (Exception e) {
            throw new InvalidRequestException(e.getMessage());
        }
    }
}
