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
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.util.List;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LuceneQueryProcessor {

    public ResultMessage proccess(IndexSearcher searcher,
                                  List<IndexExpression> expressions,
                                  SelectStatement statement,
                                  QueryState state,
                                  QueryOptions options) throws RequestExecutionException, RequestValidationException {

        int limit = statement.getLimit(options);
        int pageSize = options.getPageSize();
        boolean paging = pageSize >= 0 && pageSize < limit;
        Search search = searcher.search(expressions);

        if (paging && search.usesRelevanceOrSorting()) {
            throw new InvalidRequestException("Cannot page Lucene searches with relevance; " +
                                              "you must either use only a filter clause, " +
                                              "or use a limit lower than the current page size (" + pageSize + "), " +
                                              "or disable paging (PAGING OFF;)");
        }

        return statement.execute(state, options);
    }
}
