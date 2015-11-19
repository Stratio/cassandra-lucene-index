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

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

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

    public CassandraUtils refresh() {
        parent.execute(update);
        parent.refresh();
        return parent;
    }
}
