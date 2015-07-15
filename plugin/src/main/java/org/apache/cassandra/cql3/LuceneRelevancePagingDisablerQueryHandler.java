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
import com.stratio.cassandra.lucene.search.Search;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

/**
 * {@link QueryHandler} that disables paging on relevance queries.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LuceneRelevancePagingDisablerQueryHandler extends LuceneQueryHandler {

    @Override
    public ResultMessage proccess(IndexSearcher searcher,
                                  List<IndexExpression> expressions,
                                  SelectStatement statement,
                                  QueryState state,
                                  QueryOptions options) throws RequestExecutionException, RequestValidationException {

        int limit = statement.getLimit(options);
        int pageSize = options.getPageSize();
        boolean paging = pageSize >= 0 && pageSize < limit;
        Search search = searcher.search(expressions);

        if (!paging || !search.usesRelevanceOrSorting()) {
            return statement.execute(state, options);
        }

        String keyspace = statement.keyspace();
        String columnFamily = statement.columnFamily();
        long now = System.currentTimeMillis();

        ConsistencyLevel cl = options.getConsistency();
        if (cl == null) throw new InvalidRequestException("Invalid empty consistency level");
        cl.validateForRead(keyspace);

        IDiskAtomFilter filter = makeFilter(statement, options, limit);
        AbstractBounds<RowPosition> keyBounds = statement.getKeyBounds(options);
        boolean countCQL3Rows = isDistinct(statement);

        RangeSliceCommand command = new RangeSliceCommand(keyspace,
                                                          columnFamily,
                                                          now,
                                                          filter,
                                                          keyBounds,
                                                          expressions,
                                                          limit,
                                                          countCQL3Rows,
                                                          false);

        List<Row> rows = StorageProxy.getRangeSlice(command, cl);

        return statement.processResults(rows, options, limit, now);
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

    private boolean isDistinct(SelectStatement statement) throws InvalidRequestException {
        try {
            Field field = SelectStatement.Parameters.class.getDeclaredField("isDistinct");
            field.setAccessible(true);
            return (boolean) field.get(statement.parameters);
        } catch (Exception e) {
            e.printStackTrace();
            throw new InvalidRequestException(e.getMessage());
        }
    }

}
