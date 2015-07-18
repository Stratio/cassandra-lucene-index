/*
 * Copyright 2015, Stratio.
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
package org.apache.cassandra.cql3;

import com.stratio.cassandra.lucene.IndexSearcher;
import com.stratio.cassandra.lucene.util.Log;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Abstract {@link QueryHandler} to be used with Lucene searches.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LuceneQueryHandler implements QueryHandler {

    static QueryProcessor cqlProcessor = QueryProcessor.instance;

    public LuceneQueryHandler() {
        System.out.println("INSTANTIATING");
    }

    @Override
    public ResultMessage.Prepared prepare(String query, QueryState state) throws RequestValidationException {
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
    public ResultMessage processPrepared(CQLStatement statement, QueryState state, QueryOptions options)
    throws RequestExecutionException, RequestValidationException {
        return cqlProcessor.processPrepared(statement, state, options);
    }

    @Override
    public ResultMessage processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options)
    throws RequestExecutionException, RequestValidationException {
        return cqlProcessor.processBatch(statement, state, options);
    }

    @Override
    public ResultMessage process(String query, QueryState state, QueryOptions options)
    throws RequestExecutionException, RequestValidationException {
        System.out.println("PROCESSING LUCENE");

        ParsedStatement.Prepared p = QueryProcessor.getStatement(query, state.getClientState());
        options.prepare(p.boundNames);
        CQLStatement prepared = p.statement;
        if (prepared.getBoundTerms() != options.getValues().size())
            throw new InvalidRequestException("Invalid amount of bind variables");

        if (!state.getClientState().isInternal) QueryProcessor.metrics.regularStatementsExecuted.inc();

        if (prepared instanceof SelectStatement) {
            SelectStatement select = (SelectStatement) prepared;
            List<IndexExpression> expressions = select.getValidatedIndexExpressions(options);
            ColumnFamilyStore cfs = Keyspace.open(select.keyspace()).getColumnFamilyStore(select.columnFamily());
            SecondaryIndexManager secondaryIndexManager = cfs.indexManager;
            SecondaryIndexSearcher searcher = secondaryIndexManager.getHighestSelectivityIndexSearcher(expressions);
            if (searcher != null && searcher instanceof IndexSearcher) {
                try {
                    return proccess((IndexSearcher) searcher, expressions, select, state, options);
                } catch (RequestExecutionException | RequestValidationException e) {
                    throw e;
                } catch (Exception e) {
                    Log.error(e, e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        return cqlProcessor.processStatement(prepared, state, options);
    }

    public ResultMessage proccess(IndexSearcher searcher,
                                  List<IndexExpression> expressions,
                                  SelectStatement statement,
                                  QueryState state,
                                  QueryOptions options) throws Exception {
        Log.info("EXECUTING HANDLER " + this + " WITH OPTION CLASS " +options.getClass() +  " WITH PAGING STATE " + options.getPagingState());

        ClientState clientState = state.getClientState();
        statement.checkAccess(clientState);
        statement.validate(clientState);

        int limit = statement.getLimit(options);
        int page = options.getPageSize();

        String ks = statement.keyspace();
        String cf = statement.columnFamily();
        long now = System.currentTimeMillis();

        ConsistencyLevel cl = options.getConsistency();
        if (cl == null) throw new InvalidRequestException("Invalid empty consistency level");
        cl.validateForRead(ks);

        IDiskAtomFilter filter = makeFilter(statement, options, limit);
        AbstractBounds<RowPosition> range = statement.getKeyBounds(options);

        List<Row> rows = LuceneQueryProcessor.run(searcher, ks, cf, now, filter, range, expressions, limit, cl, page);

        PagingState pagingState = new PagingState(null, null, limit);
        ResultMessage.Rows msg = statement.processResults(rows, options, limit, now);
//        msg.result.metadata.setHasMorePages(pagingState);
        return msg;
    }

    private IDiskAtomFilter makeFilter(SelectStatement statement, QueryOptions options, int limit)
    throws InvalidRequestException {
        try {
            Method method = SelectStatement.class.getDeclaredMethod("makeFilter", QueryOptions.class, int.class);
            method.setAccessible(true);
            return (IDiskAtomFilter) method.invoke(statement, options, limit);
        } catch (Exception e) {
            e.printStackTrace();
            throw new InvalidRequestException(e.getMessage());
        }
    }
}