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

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.cassandra.lucene.builder.index.Index;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.stratio.cassandra.lucene.builder.Builder.all;
import static com.stratio.cassandra.lucene.builder.Builder.index;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraConfig.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraUtils {

    protected static final Logger logger = BaseAT.logger;

    private final String keyspace;
    private final String table;
    private final String index;
    private final String qualifiedTable;
    private final Map<String, String> columns;
    private final Map<String, Mapper> mappers;
    private final List<String> partitionKey;
    private final List<String> clusteringKey;
    private final Map<String, Map<String, String>> udts;

    public static CassandraUtilsBuilder builder(String name) {
        return new CassandraUtilsBuilder(name);
    }

    public CassandraUtils(String keyspace,
                          String table,
                          String index,
                          Map<String, String> columns,
                          Map<String, Mapper> mappers,
                          List<String> partitionKey,
                          List<String> clusteringKey,
                          Map<String, Map<String, String>> udts) {

        this.keyspace = keyspace;
        this.table = table;
        this.index = index;
        this.columns = columns;
        this.mappers = mappers;
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
        this.udts = udts;
        qualifiedTable = keyspace + "." + table;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return table;
    }

    public String getQualifiedTable() {
        return qualifiedTable;
    }

    public String getIndex() {
        return index;
    }

    public Statement statement(String query, Object... args) {
        return new SimpleStatement(String.format(query, args));
    }

    public ResultSet execute(Statement statement) {
        return CassandraConnection.execute(statement);
    }

    public ResultSet execute(String query) {
        if (!query.endsWith(";")) {
            query += ";";
        }
        return execute(new SimpleStatement(query));
    }

    public ResultSet execute(String query, String... args) {
        return execute(String.format(query, args));
    }

    ResultSet execute(StringBuilder query) {
        return execute(query.toString());
    }

    public CassandraUtils waitForIndexing() {

        // Waiting for the custom index to be refreshed
        logger.debug("Waiting for the index to be created...");
        try {
            TimeUnit.SECONDS.sleep(WAIT_FOR_INDEXING);
        } catch (InterruptedException e) {
            logger.error("Interruption caught during a Thread.sleep; index might be unstable");
        }
        logger.debug("Index ready to rock!");

        return this;
    }

    public CassandraUtils refresh() {
        select().query(Search.none()).refresh(true).consistency(ConsistencyLevel.ALL).limit(1).getFirst();
        return this;
    }

    public CassandraUtils createKeyspace() {
        execute(new StringBuilder().append("CREATE KEYSPACE ")
                                   .append(keyspace)
                                   .append(" with replication = ")
                                   .append("{'class' : 'SimpleStrategy', 'replication_factor' : '")
                                   .append(REPLICATION)
                                   .append("' };"));
        return this;
    }

    public CassandraUtils dropTable() {
        execute("DROP TABLE " + qualifiedTable);
        return this;
    }

    public CassandraUtils dropKeyspace() {
        execute("DROP KEYSPACE " + keyspace + " ;");
        return this;
    }

    public CassandraUtils createTable() {
        StringBuilder sb = new StringBuilder().append("CREATE TABLE ").append(qualifiedTable).append(" (");
        for (String s : columns.keySet()) {
            sb.append(s).append(" ").append(columns.get(s)).append(", ");
        }
        sb.append("PRIMARY KEY ((");
        for (int i = 0; i < partitionKey.size(); i++) {
            sb.append(partitionKey.get(i));
            sb.append(i == partitionKey.size() - 1 ? ")" : ",");
        }
        for (String s : clusteringKey) {
            sb.append(", ").append(s);
        }
        sb.append("))");
        execute(sb);
        return this;
    }

    public CassandraUtils createUDTs() {
        for (Map.Entry<String, Map<String, String>> entry : udts.entrySet()) {
            String name = entry.getKey();
            Map<String, String> map = entry.getValue();
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TYPE ").append(keyspace).append(".").append(name).append(" ( ");
            Set<String> set = map.keySet();
            Iterator<String> iterator = set.iterator();
            for (int i = 0; i < set.size(); i++) {
                String key = iterator.next();
                sb.append(key).append(" ").append(map.get(key));
                if (i < (set.size() - 1)) {
                    sb.append(", ");
                }
            }
            sb.append(");");
            execute(sb);
        }
        return this;
    }

    public CassandraUtils truncateTable() {
        execute(new StringBuilder().append("TRUNCATE ").append(qualifiedTable));
        return this;
    }

    public CassandraUtils createIndex() {
        Index index = index(keyspace, table, this.index).refreshSeconds(REFRESH)
                                                        .indexingThreads(THREADS)
                                                        .tokenRangeCacheSize(TOKEN_RANGE_CACHE_SIZE)
                                                        .searchCacheSize(SEARCH_CACHE_SIZE);
        for (Map.Entry<String, Mapper> entry : mappers.entrySet()) {
            index.mapper(entry.getKey(), entry.getValue());
        }
        execute(index.build());
        return this;
    }

    public CassandraUtils dropIndex() {
        execute(new StringBuilder().append("DROP INDEX ").append(keyspace).append(".").append(index).append(";"));
        return this;
    }

    public CassandraUtilsSelect selectAllFromIndexQueryWithFiltering(int limit, String name, Object value) {
        return searchAll().andEq(name, value).limit(limit).allowFiltering(true);
    }

    @SafeVarargs
    public final CassandraUtils insert(Map<String, String>... paramss) {

        Batch batch = QueryBuilder.unloggedBatch();
        for (Map<String, String> params : paramss) {
            String columns = "";
            String values = "";
            for (String s : params.keySet()) {
                columns += s + ",";
                values = values + params.get(s) + ",";
            }
            columns = columns.substring(0, columns.length() - 1);
            values = values.substring(0, values.length() - 1);

            batch.add(new SimpleStatement(String.format("INSERT INTO %s (%s) VALUES (%s);",
                                                        qualifiedTable,
                                                        columns,
                                                        values)));
        }
        execute(batch);
        return this;
    }

    public CassandraUtils insert(String[] names, Object[] values) {
        execute(QueryBuilder.insertInto(keyspace, table).values(names, values));
        return this;
    }

    public CassandraUtilsDelete delete(String... names) {
        return new CassandraUtilsDelete(this, names);
    }

    public CassandraUtilsUpdate update() {
        return new CassandraUtilsUpdate(this);
    }

    public CassandraUtilsSelect select() {
        return new CassandraUtilsSelect(this);
    }

    public CassandraUtilsSelect searchAll() {
        return select().search().filter(all());
    }

    public CassandraUtilsSelect query(Condition query) {
        return select().query(query);
    }

    public CassandraUtilsSelect filter(Condition filter) {
        return select().filter(filter);
    }

    public CassandraUtilsSelect sort(SortField... sort) {
        return select().sort(sort);
    }

    public List<Row> searchWithPreparedStatement(Search search) {
        String query = String.format("SELECT * FROM %s WHERE expr(%s,?) LIMIT %d", qualifiedTable, index, LIMIT);
        final PreparedStatement stmt = CassandraConnection.session.prepare(query);
        BoundStatement b = stmt.bind();
        b.setString(0, search.build());
        return execute(b).all();
    }

}
