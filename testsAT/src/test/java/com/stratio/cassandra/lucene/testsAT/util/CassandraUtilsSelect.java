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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.stratio.cassandra.lucene.testsAT.util.CassandraConfig.FETCH;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraConfig.LIMIT;
import static org.junit.Assert.*;

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
    private boolean refresh = true;
    private boolean allowFiltering = false;
    private ConsistencyLevel consistency;

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
            sb.append(String.format("expr(%s,'%s')", parent.getIndex(), search.refresh(refresh).build()));
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

    public int count() {
        return get().size();
    }

    public CassandraUtils check(int expected) {
        assertEquals(String.format("Expected %d results!", expected), expected, get().size());
        return parent;
    }

    @SuppressWarnings("unchecked")
    public <T> CassandraUtils check(String column, Class<T> clazz, T... expecteds) {
        List<Row> rows = get();
        List<T> values = new ArrayList<>();
        for (Row row : rows) {
            T value = row.get(column, clazz);
            values.add(value);
        }
        T[] actuals = (T[]) Array.newInstance(clazz, values.size());
        values.toArray(actuals);
        assertArrayEquals("Expected different values", expecteds, actuals);
        return parent;
    }

    public <T extends Exception> CassandraUtils check(Class<T> expected) {
        try {
            get();
            fail("Search should have been invalid!");
        } catch (Exception e) {
            assertTrue(String.format("Exception should be %s but found %s",
                                     expected.getSimpleName(),
                                     e.getClass().getSimpleName()), expected.isAssignableFrom(e.getClass()));
        }
        return parent;
    }

    public CassandraUtils checkIntColumn(String name, int... expected) {
        List<Row> rows = get();
        assertEquals(String.format("Expected %d results!", expected.length), expected.length, rows.size());
        int[] actual = new int[expected.length];
        for (int i = 0; i < expected.length; i++) {
            actual[i] = rows.get(i).getInt(name);
        }
        assertArrayEquals(String.format("Expected %s but found %s", Arrays.toString(expected), Arrays.toString(actual)),
                          expected,
                          actual);
        return parent;
    }

    public CassandraUtils checkStringColumn(String name, String... expected) {
        List<Row> rows = get();
        assertEquals(String.format("Expected %d results!", expected.length), expected.length, rows.size());
        String[] actual = new String[expected.length];
        for (int i = 0; i < expected.length; i++) {
            actual[i] = rows.get(i).getString(name);
        }
        assertArrayEquals(String.format("Expected %s but found %s", Arrays.toString(expected), Arrays.toString(actual)),
                          expected,
                          actual);
        return parent;
    }

    public CassandraUtils checkStringColumnWithoutOrder(String name, String... expected) {
        List<Row> rows = get();
        assertEquals(String.format("Expected %d results!", expected.length), expected.length, rows.size());
        String[] actual = new String[expected.length];
        for (int i = 0; i < expected.length; i++) {
            actual[i] = rows.get(i).getString(name);
        }
        Arrays.sort(expected);
        Arrays.sort(actual);
        assertArrayEquals(String.format("Expected %s but found %s", Arrays.toString(expected), Arrays.toString(actual)),
                          expected,
                          actual);
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
