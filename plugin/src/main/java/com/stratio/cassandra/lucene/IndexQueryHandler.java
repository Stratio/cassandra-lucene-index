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

import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.util.TimeCounter;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.LuceneStorageProxy;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.db.filter.RowFilter.Expression;
import static org.apache.cassandra.transport.messages.ResultMessage.Rows;

/**
 * {@link QueryHandler} to be used with Lucene searches.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class IndexQueryHandler implements QueryHandler {

    private static final Logger logger = LoggerFactory.getLogger(IndexQueryHandler.class);

    private static Method getPageSize;
    private static Method processResults;

    static {
        try {
            getPageSize = SelectStatement.class.getDeclaredMethod("getPageSize", QueryOptions.class);
            getPageSize.setAccessible(true);
            processResults = SelectStatement.class.getDeclaredMethod("processResults",
                                                                     PartitionIterator.class,
                                                                     QueryOptions.class,
                                                                     int.class,
                                                                     int.class);
            processResults.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public ResultMessage.Prepared prepare(String query, QueryState state, Map<String, ByteBuffer> customPayload) {
        return QueryProcessor.instance.prepare(query, state);
    }

    /** {@inheritDoc} */
    @Override
    public ParsedStatement.Prepared getPrepared(MD5Digest id) {
        return QueryProcessor.instance.getPrepared(id);
    }

    /** {@inheritDoc} */
    @Override
    public ParsedStatement.Prepared getPreparedForThrift(Integer id) {
        return QueryProcessor.instance.getPreparedForThrift(id);
    }

    /** {@inheritDoc} */
    @Override
    public ResultMessage processBatch(BatchStatement statement,
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload) {
        return QueryProcessor.instance.processBatch(statement, state, options, customPayload);
    }

    /** {@inheritDoc} */
    @Override
    public ResultMessage processPrepared(CQLStatement statement,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload) {
        QueryProcessor.metrics.preparedStatementsExecuted.inc();
        return processStatement(statement, state, options);
    }

    /** {@inheritDoc} */
    @Override
    public ResultMessage process(String query,
                                 QueryState state,
                                 QueryOptions options,
                                 Map<String, ByteBuffer> customPayload) {
        ParsedStatement.Prepared p = QueryProcessor.getStatement(query, state.getClientState());
        options.prepare(p.boundNames);
        CQLStatement prepared = p.statement;
        if (prepared.getBoundTerms() != options.getValues().size()) {
            throw new InvalidRequestException("Invalid amount of bind variables");
        }

        if (!state.getClientState().isInternal) {
            QueryProcessor.metrics.regularStatementsExecuted.inc();
        }

        return processStatement(prepared, state, options);
    }

    private ResultMessage processStatement(CQLStatement statement, QueryState state, QueryOptions options) {

        logger.trace("Process {} @CL.{}", statement, options.getConsistency());
        ClientState clientState = state.getClientState();
        statement.checkAccess(clientState);
        statement.validate(clientState);

        // Intercept Lucene index searches
        if (statement instanceof SelectStatement) {
            SelectStatement select = (SelectStatement) statement;
            Map<Expression, Index> map = expressions(select, options);
            if (!map.isEmpty()) {
                TimeCounter time = TimeCounter.create().start();
                try {
                    return executeLuceneQuery(select, state, options, map);
                } catch (ReflectiveOperationException e) {
                    throw new IndexException(e);
                } finally {
                    logger.debug("Lucene search total time: {}\n", time.stop());
                }
            }
        }

        return execute(statement, state, options);
    }

    private Map<Expression, Index> expressions(SelectStatement select, QueryOptions options) {
        Map<Expression, Index> map = new LinkedHashMap<>();
        List<RowFilter.Expression> expressions = select.getRowFilter(options).getExpressions();
        ColumnFamilyStore cfs = Keyspace.open(select.keyspace()).getColumnFamilyStore(select.columnFamily());
        for (Expression expression : expressions) {
            if (expression.isCustom()) {
                RowFilter.CustomExpression customExpression = (RowFilter.CustomExpression) expression;
                String clazz = customExpression.getTargetIndex().options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
                if (clazz.equals(Index.class.getCanonicalName())) {
                    Index index = (Index) cfs.indexManager.getIndex(customExpression.getTargetIndex());
                    map.put(expression, index);
                }
            }
            cfs.indexManager.listIndexes().forEach(index -> {
                if (index instanceof Index && index.supportsExpression(expression.column(), expression.operator())) {
                    map.put(expression, (Index) index);
                }
            });
        }
        return map;
    }

    private ResultMessage execute(CQLStatement statement, QueryState state, QueryOptions options) {
        ResultMessage result = statement.execute(state, options);
        return result == null ? new ResultMessage.Void() : result;
    }

    private ResultMessage executeLuceneQuery(SelectStatement select,
                                             QueryState state,
                                             QueryOptions options,
                                             Map<Expression, Index> expressions)
    throws ReflectiveOperationException {

        if (expressions.size() > 1) {
            throw new InvalidRequestException("Lucene index only supports one search expression per query.");
        }

        // Validate expression
        Expression expression = expressions.keySet().iterator().next();
        Index index = expressions.get(expression);
        Search search = index.validate(expression);

        // Check paging
        int limit = select.getLimit(options);
        int page = (int) getPageSize.invoke(select, options);

        if (search.requiresPostProcessing() && page > 0 && page < limit) {
            return executeSortedLuceneQuery(select, state, options);
        }

        // Process
        return execute(select, state, options);
    }

    private Rows executeSortedLuceneQuery(SelectStatement select, QueryState state, QueryOptions options)
    throws ReflectiveOperationException {

        // Check consistency level
        ConsistencyLevel cl = options.getConsistency();
        checkNotNull(cl, "Invalid empty consistency level");
        cl.validateForRead(select.keyspace());

        int nowInSec = FBUtilities.nowInSeconds();
        int limit = select.getLimit(options);
        int page = options.getPageSize();

        // Read paging state and write it to query
        IndexPagingState pagingState = IndexPagingState.build(options.getPagingState(), limit);
        ReadQuery query = select.getQuery(options, nowInSec, Math.min(page, pagingState.remaining()));
        pagingState.rewrite(query);

        try (PartitionIterator data = data(query, cl, state)) {
            PartitionIterator processedData = pagingState.update(query, data, options.getConsistency());
            Rows rows = (Rows) processResults.invoke(select, processedData, options, nowInSec, page);
            rows.result.metadata.setHasMorePages(pagingState.toPagingState());
            return rows;
        }
    }

    private PartitionIterator data(ReadQuery query, ConsistencyLevel cl, QueryState state)
    throws ReflectiveOperationException {
        if (query instanceof SinglePartitionReadCommand.Group) {
            SinglePartitionReadCommand.Group group = (SinglePartitionReadCommand.Group) query;
            if (group.commands.size() > 1) {
                return LuceneStorageProxy.read(group, cl);
            }
        }
        return query.execute(cl, state.getClientState());
    }

}
