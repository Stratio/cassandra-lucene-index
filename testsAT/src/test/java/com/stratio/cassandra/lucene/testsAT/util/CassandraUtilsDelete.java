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

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraUtilsDelete {

    private final CassandraUtils parent;
    private final Delete.Where where;
    private CassandraUtilsDelete carry;

    public CassandraUtilsDelete(CassandraUtils parent, String... columns) {
        this(parent, null, columns);
    }

    public CassandraUtilsDelete(CassandraUtils parent, CassandraUtilsDelete carry, String... columns) {
        this.parent = parent;
        this.carry = carry;
        where = QueryBuilder.delete(columns).from(parent.getKeyspace(), parent.getTable()).where();
    }

    public CassandraUtilsDelete where(String name, Object value) {
        where.and(QueryBuilder.eq(name, value));
        return this;
    }

    public CassandraUtilsDelete and(String name, Object value) {
        return where(name, value);
    }

    public CassandraUtilsDelete delete(String... columns) {
        return new CassandraUtilsDelete(parent, this, columns);
    }

    private CassandraUtils execute() {
        if (carry != null) {
            carry.execute();
        }
        parent.execute(where);
        return parent;
    }

    public CassandraUtilsSelect query(Condition query) {
        return execute().query(query);
    }

    public CassandraUtilsSelect filter(Condition filter) {
        return execute().filter(filter);
    }

    public CassandraUtilsSelect sort(SortField... sort) {
        return execute().sort(sort);
    }

    public CassandraUtils refresh() {
        return execute().commit().refresh();
    }
}
