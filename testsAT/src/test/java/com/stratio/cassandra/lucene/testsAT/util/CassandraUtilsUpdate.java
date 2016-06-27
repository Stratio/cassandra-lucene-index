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

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraUtilsUpdate {

    private CassandraUtils parent;
    private Update update;

    public CassandraUtilsUpdate(CassandraUtils parent) {
        this.parent = parent;
        update = QueryBuilder.update(parent.getKeyspace(), parent.getTable());
    }

    public CassandraUtilsUpdate set(String name, Object value) {
        update.with(QueryBuilder.set(name, value));
        return this;
    }

    public CassandraUtilsUpdate where(String name, Object value) {
        update.where(QueryBuilder.eq(name, value));
        return this;
    }

    public CassandraUtilsUpdate and(String name, Object value) {
        update.where().and(QueryBuilder.eq(name, value));
        return this;
    }

    public Update.Conditions onlyIf(Clause condition) {
        return update.onlyIf(condition);
    }

    private CassandraUtils execute() {
        parent.execute(update);
        return parent;
    }

    public Update asUpdate() {
        return update;
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
