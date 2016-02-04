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
import com.stratio.cassandra.lucene.util.TimeCounter;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * {@link QueryHandler} to be used with Lucene searches.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexQueryHandler implements QueryHandler {

    private static final Logger logger = LoggerFactory.getLogger(IndexQueryHandler.class);

    @Override
    /** {@inheritDoc} */
    public ResultMessage.Prepared prepare(String query,
                                          QueryState state,
                                          Map<String, ByteBuffer> customPayload) throws RequestValidationException {
        return QueryProcessor.instance.prepare(query, state);
    }

    @Override
    /** {@inheritDoc} */
    public ParsedStatement.Prepared getPrepared(MD5Digest id) {
        return QueryProcessor.instance.getPrepared(id);
    }

    @Override
    /** {@inheritDoc} */
    public ParsedStatement.Prepared getPreparedForThrift(Integer id) {
        return QueryProcessor.instance.getPreparedForThrift(id);
    }

    @Override
    /** {@inheritDoc} */
    public ResultMessage processBatch(BatchStatement statement,
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload)
    throws RequestExecutionException, RequestValidationException {
        return QueryProcessor.instance.processBatch(statement, state, options, customPayload);
    }

    @Override
    /** {@inheritDoc} */
    public ResultMessage processPrepared(CQLStatement statement,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload)
    throws RequestExecutionException, RequestValidationException {
        QueryProcessor.metrics.preparedStatementsExecuted.inc();
        return processStatement(statement, state, options);
    }

    @Override
    /** {@inheritDoc} */
    public ResultMessage process(String query,
                                 QueryState state,
                                 QueryOptions options,
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

        return processStatement(prepared, state, options);
    }

    public ResultMessage processStatement(CQLStatement statement, QueryState state, QueryOptions options)
    throws RequestExecutionException, RequestValidationException {

        logger.trace("Process {} @CL.{}", statement, options.getConsistency());
        ClientState clientState = state.getClientState();
        statement.checkAccess(clientState);
        statement.validate(clientState);

        // Intercept Lucene index searches
        if (statement instanceof SelectStatement) {
            SelectStatement select = (SelectStatement) statement;
            List<RowFilter.Expression> expressions = select.getRowFilter(options).getExpressions();
            for (RowFilter.Expression expression : expressions) {
                if (expression.isCustom()) {
                    RowFilter.CustomExpression customExpression = (RowFilter.CustomExpression) expression;
                    String clazz = customExpression.getTargetIndex().options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
                    if (clazz.equals(Index.class.getCanonicalName())) {
                        return process(select, state, options, customExpression);
                    }
                }
            }
        }

        ResultMessage result = statement.execute(state, options);
        return result == null ? new ResultMessage.Void() : result;
    }

    private ResultMessage process(SelectStatement select,
                                  QueryState state,
                                  QueryOptions options,
                                  RowFilter.CustomExpression expression) {
        TimeCounter time = TimeCounter.create().start();
        try {
            int limit = select.getLimit(options);
            int page = options.getPageSize();
            if (page > 0 && limit > page) {
                String json = UTF8Type.instance.compose(expression.getValue());
                Search search = SearchBuilder.fromJson(json).build();
                if (search.isTopK()) {
                    String msg = String.format("Paging is not allowed for top-k searches as %s. " +
                                               "You should specify a limit (%d) lower than page size (%d) " +
                                               "or consider using an unsorted filter instead.", json, limit, page);
                    throw new InvalidRequestException(msg);
                }
            }
            return select.execute(state, options);
        } finally {
            logger.debug("Total Lucene query time: {}\n", time.stop());
        }
    }

}
