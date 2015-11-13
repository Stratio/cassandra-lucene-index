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

package com.stratio.cassandra.lucene.util;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.index.Index;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.stratio.cassandra.lucene.TestingConstants.*;
import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraUtils {

    protected static final Logger logger = BaseTest.logger;

    private static Session session;

    private final ConsistencyLevel consistencyLevel;
    private final String keyspace;
    private final String table;
    private final String index;
    private final Integer fetchSize;
    private final String qualifiedTable;
    private final Map<String, String> columns;
    private final Map<String, Mapper> mappers;
    private final List<String> partitionKey;
    private final List<String> clusteringKey;
    private final String indexColumn;
    private final String replicationFactor;

    public static CassandraUtilsBuilder builder(String name) {
        return new CassandraUtilsBuilder(name);
    }

    public CassandraUtils(String host,
                          String keyspace,
                          String table,
                          String index,
                          Integer fetchSize,
                          Map<String, String> columns,
                          Map<String, Mapper> mappers,
                          List<String> partitionKey,
                          List<String> clusteringKey,
                          String indexColumn) {

        String consistencyLevelString = System.getProperty(CONSISTENCY_LEVEL_CONSTANT_NAME);

        if (consistencyLevelString == null) {
            consistencyLevelString = DEFAULT_CONSISTENCY_LEVEL;
        }

        consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelString);

        prepareSession(host, consistencyLevel);

        String replicationFactorString = System.getProperty(REPLICATION_FACTOR_CONSTANT_NAME);

        if (replicationFactorString == null || Integer.parseInt(replicationFactorString) < 1) {
            replicationFactorString = DEFAULT_REPLICATION_FACTOR;
        }

        this.replicationFactor = replicationFactorString;

        this.keyspace = keyspace;
        this.table = table;
        this.index = index;
        this.fetchSize = fetchSize;
        this.columns = columns;
        this.mappers = mappers;
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
        this.indexColumn = indexColumn;
        qualifiedTable = keyspace + "." + table;
    }

    public static synchronized void prepareSession(String host,ConsistencyLevel consistencyLevel) {
        if (session == null) {
            Cluster cluster = Cluster.builder().addContactPoint(host).build();
            cluster.getConfiguration().getQueryOptions().setConsistencyLevel(consistencyLevel);
            cluster.getConfiguration().getQueryOptions().setFetchSize(Integer.MAX_VALUE);
            cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(600000);
            session = cluster.connect();
        }
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return table;
    }

    public String getIndexColumn() {
        return indexColumn;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    protected List<Row> execute(StringBuilder sb) {
        return execute(sb.toString(), fetchSize).all();
    }

    protected List<Row> execute(StringBuilder sb, int fetchSize) {
        return execute(sb.toString(), fetchSize).all();
    }

    protected List<Row> execute(Statement statement) {
        return execute(statement.toString(), fetchSize).all();
    }

    protected List<Row> execute(Statement statement, int fetchSize) {
        return execute(statement.toString(), fetchSize).all();
    }

    public List<Row> execute(String query) {
        return execute(query, fetchSize).all();
    }

    public ResultSet executeQuery(String query) {
        return execute(query, fetchSize);
    }

    protected ResultSet execute(String query, int fetchSize) {
        if (!query.endsWith(";")) {
            query += ";";
        }
        logger.debug("CQL: " + query);
        Statement statement = new SimpleStatement(query);
        statement.setFetchSize(fetchSize).setConsistencyLevel(consistencyLevel);
        return session.execute(statement);
    }


    public Session getSession() {
        return session;
    }

    public CassandraUtils waitForIndexing() {

        // Waiting for the custom index to be refreshed
        logger.debug("Waiting for the index to be created...");
        try {
            Thread.sleep(INDEX_WAIT_MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Interruption caught during a Thread.sleep; index might be unstable");
        }
        logger.debug("Index ready to rock!");

        return this;
    }

    public CassandraUtils refresh() {
        session.execute(QueryBuilder.select()
                                    .from(keyspace, table)
                                    .where(eq(indexColumn, Builder.search().refresh(true).build()))
                                    .setConsistencyLevel(ConsistencyLevel.ALL));
        return this;
    }

    public CassandraUtils createKeyspace() {
        execute(new StringBuilder().append("CREATE KEYSPACE ")
                                   .append(keyspace)
                                   .append(" with replication = ")
                                   .append("{'class' : 'SimpleStrategy', 'replication_factor' : '")
                                   .append(replicationFactor)
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

    public CassandraUtils truncateTable() {
        execute(new StringBuilder().append("TRUNCATE ").append(qualifiedTable));
        return this;
    }

    public CassandraUtils createIndex() {
        Index index = index(keyspace, table, indexColumn).name(this.index)
                                                         .refreshSeconds(INDEX_REFRESH_SECONDS)
                                                         .indexingThreads(INDEXING_THREADS);
        for (Map.Entry<String, Mapper> entry : mappers.entrySet()) {
            index.mapper(entry.getKey(), entry.getValue());
        }
        execute(index.build());
        return this;
    }

    public CassandraUtils dropIndex() {
        execute(new StringBuilder().append("DROP INDEX ")
                                   .append(keyspace)
                                   .append(".")
                                   .append(index)
                                   .append(";"));
        return this;
    }

    public List<Row> selectAllFromIndexQueryWithFiltering(int limit, String name, Object value) {
        Search search = Builder.search().query(all()).refresh(true);
        return execute(QueryBuilder.select()
                                   .from(keyspace, table)
                                   .where(eq(indexColumn, search.build()))
                                   .and(eq(name, value))
                                   .limit(limit)
                                   .allowFiltering()
                                   .setConsistencyLevel(consistencyLevel));
    }

    @SafeVarargs
    public final CassandraUtils insert(Map<String, String>... paramss) {

        Batch batch = QueryBuilder.unloggedBatch();
        for (Map<String, String> params : paramss) {
            String columns = "";
            String values = "";
            for (String s : params.keySet()) {
                if (!s.equals(indexColumn)) {
                    columns += s + ",";
                    values = values + params.get(s) + ",";
                }
            }
            columns = columns.substring(0, columns.length() - 1);
            values = values.substring(0, values.length() - 1);

            batch.add(new SimpleStatement(new StringBuilder().append("INSERT INTO ")
                                                             .append(qualifiedTable)
                                                             .append(" (")
                                                             .append(columns)
                                                             .append(") VALUES (")
                                                             .append(values)
                                                             .append(");")
                                                             .toString()));
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

}
