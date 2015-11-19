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

package com.stratio.cassandra.lucene.testsAT.util;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;

import java.util.LinkedList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraUtilsSelect {

    private CassandraUtils parent;
    private Search search;
    private LinkedList<Clause> clauses;
    private LinkedList<String> extras;
    private Integer limit;
    private Integer fetchSize;
    private Boolean refresh = true;

    public CassandraUtilsSelect(CassandraUtils parent) {
        this.parent = parent;
        clauses = new LinkedList<>();
        extras = new LinkedList<>();
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

    public CassandraUtilsSelect query(Condition condition) {
        if (search == null) {
            search = Builder.search().query(condition);
        } else {
            search.query(condition);
        }
        return this;
    }

    public CassandraUtilsSelect filter(Condition condition) {
        if (search == null) {
            search = Builder.search().filter(condition);
        } else {
            search.filter(condition);
        }
        return this;
    }

    public CassandraUtilsSelect sort(SortField... sort) {
        if (search == null) {
            search = Builder.search().sort(sort);
        } else {
            search.sort(sort);
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

    public List<Row> get() {
        Select.Where where = QueryBuilder.select().from(parent.getKeyspace(), parent.getTable()).where();
        for (Clause clause : clauses) {
            where.and(clause);
        }
        if (search != null) {
            where.and(eq(parent.getIndexColumn(), search.refresh(refresh).build()));
        }
        Statement statement = limit == null ? where : where.limit(limit);

        String query = statement.toString();
        query = query.substring(0, query.length() - 1);
        StringBuilder sb = new StringBuilder(query);
        for (String extra : extras) {
            sb.append(" ");
            sb.append(extra);
            sb.append(" ");
        }
        statement = new SimpleStatement(sb.toString());
        if (fetchSize != null) {
            statement.setFetchSize(fetchSize);
        }
        return parent.execute(statement).all();
    }

    public Row getFirst() {
        List<Row> rows = get();
        return rows.isEmpty() ? null : rows.get(0);
    }

    public int count() {
        return get().size();
    }

    public CassandraUtils check(int expected) {
        assertEquals(String.format("Expected %d results!", expected), expected, get().size());
        return parent;
    }

    public <T extends Exception> CassandraUtils check(Class<T> expected) {
        try {
            get();
            fail("Search should have been invalid!");
        } catch (Exception e) {
            assertTrue("Exception should be " + expected.getSimpleName(), expected.isAssignableFrom(e.getClass()));
        }
        return parent;
    }

    public Integer[] intColumn(String name) {
        List<Row> rows = get();
        Integer[] values = new Integer[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getInt(name);
        }
        return values;
    }

    public Long[] longColumn(String name) {
        List<Row> rows = get();
        Long[] values = new Long[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getLong(name);
        }
        return values;
    }

    public Float[] floatColumn(String name) {
        List<Row> rows = get();
        Float[] values = new Float[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getFloat(name);
        }
        return values;
    }

    public String[] stringColumn(String name) {
        List<Row> rows = get();
        String[] values = new String[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getString(name);
        }
        return values;
    }

    public Double[] doubleColumn(String name) {
        List<Row> rows = get();
        Double[] values = new Double[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getDouble(name);
        }
        return values;
    }
}
