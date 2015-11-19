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

import com.stratio.cassandra.lucene.service.RowKeys;
import com.stratio.cassandra.lucene.service.RowMapper;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import com.stratio.cassandra.lucene.util.TimeCounter;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.LuceneStorageProxy;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Abstract {@link QueryHandler} to be used with Lucene searches.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexQueryHandler implements QueryHandler {

    private static final Logger logger = LoggerFactory.getLogger(IndexQueryHandler.class);

    static QueryProcessor cqlProcessor = QueryProcessor.instance;

    private IDiskAtomFilter makeFilter(SelectStatement statement, QueryOptions options, int limit)
    throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Method method = SelectStatement.class.getDeclaredMethod("makeFilter", QueryOptions.class, int.class);
        method.setAccessible(true);
        return (IDiskAtomFilter) method.invoke(statement, options, limit);
    }

    private static boolean hasAnyAggregateFunctions(SelectStatement selectStatement) throws Exception {
        if (selectStatement.getFunctions() != null) {
            Iterator<Function> functions = selectStatement.getFunctions().iterator();
            while (functions.hasNext()) {
                Function function = functions.next();

                if (function.isAggregate()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public ResultMessage.Prepared prepare(String query, QueryState state, Map<String, ByteBuffer> customPayload)
    throws RequestValidationException {
        return cqlProcessor.prepare(query, state);
    }

    @Override
    public ParsedStatement.Prepared getPrepared(MD5Digest id) {
        return cqlProcessor.getPrepared(id);
    }

    @Override
    public ParsedStatement.Prepared getPreparedForThrift(Integer id) {
        return cqlProcessor.getPreparedForThrift(id);
    }

    @Override
    public ResultMessage processPrepared(CQLStatement statement, QueryState state, QueryOptions options,
                                         Map<String, ByteBuffer> customPayload)
    throws RequestExecutionException, RequestValidationException {
        return cqlProcessor.processPrepared(statement, state, options);
    }

    @Override
    public ResultMessage processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload)
    throws RequestExecutionException, RequestValidationException {
        return cqlProcessor.processBatch(statement, state, options);
    }

    @Override
    public ResultMessage process(String query, QueryState state, QueryOptions options,
                                 Map<String, ByteBuffer> customPayload)
    throws RequestExecutionException, RequestValidationException {

        ParsedStatement.Prepared p = QueryProcessor.getStatement(query, state.getClientState());
        options.prepare(p.boundNames);
        CQLStatement prepared = p.statement;
        if (prepared.getBoundTerms() != options.getValues().size()) {
            throw new InvalidRequestException("Invalid amount of bind variables");
        }

        if (!state.getClientState().isInternal) {
            QueryProcessor.metrics.regularStatementsExecuted.inc();
        }

        if (prepared instanceof SelectStatement) {
            SelectStatement select = (SelectStatement) prepared;
            List<IndexExpression> expressions = select.getValidatedIndexExpressions(options);
            ColumnFamilyStore cfs = Keyspace.open(select.keyspace()).getColumnFamilyStore(select.columnFamily());
            SecondaryIndexManager secondaryIndexManager = cfs.indexManager;
            SecondaryIndexSearcher searcher = secondaryIndexManager.getHighestSelectivityIndexSearcher(expressions);
            if (searcher instanceof IndexSearcher) {
                try {
                    TimeCounter time = TimeCounter.create().start();
                    ResultMessage msg = process((IndexSearcher) searcher, expressions, select, state, options);
                    logger.debug("Total Lucene query time: {}\n", time.stop());
                    return msg;
                } catch (RequestExecutionException | RequestValidationException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IndexException(e);
                }
            }
        }

        return cqlProcessor.processStatement(prepared, state, options);
    }

    private ResultMessage process(IndexSearcher searcher,
                                  List<IndexExpression> expressions,
                                  SelectStatement statement,
                                  QueryState state,
                                  QueryOptions options) throws Exception {

        ClientState clientState = state.getClientState();
        statement.checkAccess(clientState);
        statement.validate(clientState);

        int limit = statement.getLimit(options);
        int page = options.getPageSize();
        boolean isAggregateFunction = hasAnyAggregateFunctions(statement);

        String ks = statement.keyspace();
        String cf = statement.columnFamily();
        long now = System.currentTimeMillis();

        ConsistencyLevel cl = options.getConsistency();
        if (cl == null) {
            throw new InvalidRequestException("Invalid empty consistency level");
        }
        cl.validateForRead(ks);

        IDiskAtomFilter filter = makeFilter(statement, options, limit);
        AbstractBounds<RowPosition> range = statement.getRestrictions().getPartitionKeyBounds(options);
        RowMapper mapper = searcher.mapper();
        PagingState pagingState = options.getPagingState();
        RowKeys rowKeys = null;
        if (pagingState != null) {
            limit = pagingState.remaining;
            ByteBuffer bb = pagingState.partitionKey;
            if (!ByteBufferUtils.isEmpty(bb)) {
                rowKeys = mapper.rowKeys(bb);
            }
        }

        int rowsPerCommand = page > 0 ? page : limit;
        List<Row> rows = new ArrayList<>();
        int remaining;
        int collectedRows;

        do {
            Pair<List<Row>, RowKeys> results = LuceneStorageProxy.getRangeSlice(searcher,
                                                                                ks,
                                                                                cf,
                                                                                now,
                                                                                filter,
                                                                                range,
                                                                                expressions,
                                                                                rowsPerCommand,
                                                                                cl,
                                                                                rowKeys,
                                                                                isAggregateFunction);
            collectedRows = results.left.size();
            rows.addAll(results.left);
            rowKeys = results.right;
            remaining = limit - rows.size();

        } while (isAggregateFunction && remaining > 0 && collectedRows == rowsPerCommand);

        ResultMessage.Rows msg = statement.processResults(rows, options, limit, now);
        if (!isAggregateFunction && remaining > 0 && rows.size() == rowsPerCommand) {
            ByteBuffer bb = mapper.byteBuffer(rowKeys);
            pagingState = new PagingState(bb, null, remaining);
            msg.result.metadata.setHasMorePages(pagingState);
        }
        return msg;
    }

}