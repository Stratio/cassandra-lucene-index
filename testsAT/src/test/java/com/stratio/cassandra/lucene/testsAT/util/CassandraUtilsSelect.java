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
package com.stratio.cassandra.lucene.testsAT.util;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.stratio.cassandra.lucene.testsAT.util.CassandraConfig.FETCH;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraConfig.LIMIT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraUtilsSelect {

    private final CassandraUtils parent;
    private final LinkedList<Clause> clauses;
    private final LinkedList<String> extras;
    private Search search;
    private Integer limit;
    private Integer fetchSize;
    private boolean refresh = false;
    private boolean allowFiltering = false;
    private ConsistencyLevel consistency;
    private boolean useNewQuerySyntax;

    public CassandraUtilsSelect(CassandraUtils parent) {
        this.parent = parent;
        clauses = new LinkedList<>();
        extras = new LinkedList<>();
        useNewQuerySyntax = parent.useNewQuerySyntax();
    }

    public CassandraUtilsSelect withUseNewQuerySyntax(boolean useNewQuerySyntax) {
        this.useNewQuerySyntax = useNewQuerySyntax;
        return this;
    }

    public CassandraUtilsSelect andEq(String name, Object value) {
        clauses.add(QueryBuilder.eq(name, value));
        return this;
    }

    public CassandraUtilsSelect andGt(String name, Object value) {
        clauses.add(QueryBuilder.gt(name, value));
        return this;
    }

    public CassandraUtilsSelect andGte(String name, Object value) {
        clauses.add(QueryBuilder.gte(name, value));
        return this;
    }

    public CassandraUtilsSelect andLt(String name, Object value) {
        clauses.add(QueryBuilder.lt(name, value));
        return this;
    }

    public CassandraUtilsSelect andLte(String name, Object value) {
        clauses.add(QueryBuilder.lte(name, value));
        return this;
    }

    public CassandraUtilsSelect and(String extra) {
        extras.add(extra);
        return this;
    }

    public CassandraUtilsSelect search() {
        this.search = Builder.search();
        return this;
    }

    public CassandraUtilsSelect filter(Condition... conditions) {
        if (search == null) {
            search = Builder.search().filter(conditions);
        } else {
            search.filter(conditions);
        }
        return this;
    }

    public CassandraUtilsSelect query(Condition... conditions) {
        if (search == null) {
            search = Builder.search().query(conditions);
        } else {
            search.query(conditions);
        }
        return this;
    }

    public CassandraUtilsSelect sort(SortField... fields) {
        if (search == null) {
            search = Builder.search().sort(fields);
        } else {
            search.sort(fields);
        }
        return this;
    }

    public CassandraUtilsSelect fetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public CassandraUtilsSelect refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public CassandraUtilsSelect limit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public CassandraUtilsSelect allowFiltering(boolean allowFiltering) {
        this.allowFiltering = allowFiltering;
        return this;
    }

    public CassandraUtilsSelect consistency(ConsistencyLevel consistency) {
        this.consistency = consistency;
        return this;
    }

    public List<Row> get() {
        Select.Where where = QueryBuilder.select().from(parent.getKeyspace(), parent.getTable()).where();
        clauses.forEach(where::and);

        String query = where.toString();
        query = query.substring(0, query.length() - 1); // Remove semicolon
        StringBuilder sb = new StringBuilder(query);
        if (search != null) {
            sb.append(clauses.isEmpty() ? " WHERE " : " AND ");
            String json = search.refresh(refresh).build();
            if (useNewQuerySyntax) {
                sb.append(String.format("expr(%s,'%s')", parent.getIndexName(), json));
            } else {
                sb.append(String.format("%s = '%s'", parent.getIndexColumn(), json));
            }
        }
        for (String extra : extras) {
            sb.append(" ");
            sb.append(extra);
            sb.append(" ");
        }
        sb.append(" LIMIT ").append(limit == null ? LIMIT : limit);
        if (allowFiltering) {
            sb.append(" ALLOW FILTERING");
        }
        SimpleStatement statement = new SimpleStatement(sb.toString());
        if (consistency != null) {
            statement.setConsistencyLevel(consistency);
        }
        if (fetchSize != null) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(FETCH);
        }
        return parent.execute(statement).all();
    }

    public Row getFirst() {
        List<Row> rows = get();
        return rows.isEmpty() ? null : rows.get(0);
    }

    public Row getLast() {
        List<Row> rows = get();
        return rows.isEmpty() ? null : rows.get(rows.size() - 1);
    }

    public int count() {
        return get().size();
    }

    public CassandraUtils check(int expected) {
        assertEquals(String.format("Expected %d results!", expected), expected, get().size());
        return parent;
    }

    @SuppressWarnings("unchecked")
    private <T> CassandraUtils check(String column, boolean ordered, T... expecteds) {
        List<Row> rows = get();
        assertEquals(String.format("Expected %d results!", expecteds.length), expecteds.length, rows.size());
        if (expecteds.length > 0) {
            Object[] actuals = new Object[rows.size()];
            for (int i = 0; i < rows.size(); i++) {
                actuals[i] = rows.get(i).get(column, (Class<T>) expecteds[i].getClass());
            }
            if (!ordered) {
                Arrays.sort(expecteds);
                Arrays.sort(actuals);
            }
            assertArrayEquals(String.format("Expected %s but found %s",
                                            Arrays.toString(expecteds),
                                            Arrays.toString(actuals)), expecteds, actuals);
        }
        return parent;
    }

    public <T> CassandraUtils checkOrderedColumns(String column, T... expecteds) {
        return check(column, true, expecteds);
    }

    public <T> CassandraUtils checkUnorderedColumns(String column, T... expecteds) {
        return check(column, false, expecteds);
    }

    public <T extends Exception> CassandraUtils check(Class<T> expectedClass, String expectedMessage) {
        return parent.check(this::get, expectedClass, expectedMessage);
    }
}
