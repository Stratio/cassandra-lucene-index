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

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.cassandra.lucene.builder.index.Index;
import com.stratio.cassandra.lucene.builder.index.Partitioner;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.Analyzer;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.stratio.cassandra.lucene.builder.Builder.all;
import static com.stratio.cassandra.lucene.builder.Builder.index;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraConfig.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraConnection.*;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraUtils {

    protected static final Logger logger = BaseIT.logger;

    private final String keyspace;
    private final String table;
    private final String indexName;
    private final String indexColumn;
    private final String qualifiedTable;
    private final Map<String, String> columns;
    private final Map<String, Mapper> mappers;
    private final Map<String, Analyzer> analyzers;
    private final List<String> partitionKey;
    private final List<String> clusteringKey;
    private final Map<String, Map<String, String>> udts;
    private final boolean useNewQuerySyntax;
    private final String indexBean;
    private final String clusteringOrderColumn;
    private final boolean clusteringOrderAscending;
    private final Partitioner partitioner;

    public static CassandraUtilsBuilder builder(String name) {
        return new CassandraUtilsBuilder(name);
    }

    public CassandraUtils(String keyspace,
                          String table,
                          String indexName,
                          String indexColumn,
                          boolean useNewQuerySyntax,
                          Map<String, String> columns,
                          Map<String, Mapper> mappers,
                          Map<String, Analyzer> analyzers,
                          List<String> partitionKey,
                          List<String> clusteringKey,
                          Map<String, Map<String, String>> udts,
                          String clusteringOrderColumn,
                          boolean clusteringOrderAscending,
                          Partitioner partitioner) {

        this.keyspace = keyspace;
        this.table = table;
        this.indexName = indexName;
        this.indexColumn = indexColumn;
        this.useNewQuerySyntax = useNewQuerySyntax;
        this.columns = columns;
        this.mappers = mappers;
        this.analyzers = analyzers;
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
        this.udts = udts;
        this.clusteringOrderColumn = clusteringOrderColumn;
        this.clusteringOrderAscending = clusteringOrderAscending;
        this.partitioner = partitioner;

        qualifiedTable = keyspace + "." + table;

        if (indexColumn != null && !columns.containsKey(indexColumn)) {
            columns.put(indexColumn, "text");
        }

        indexBean = String.format("com.stratio.cassandra.lucene:type=Lucene,keyspace=%s,table=%s,index=%s",
                                  keyspace, table, indexName);
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

    public String getIndexName() {
        return indexName;
    }

    public String getIndexColumn() {
        return indexColumn;
    }

    public boolean useNewQuerySyntax() {
        return useNewQuerySyntax;
    }

    public ResultSet execute(Statement statement) {
        return CassandraConnection.execute(statement);
    }

    public ResultSet execute(int fetchSize, String query) {
        if (!query.endsWith(";")) {
            query += ";";
        }
        return execute(new SimpleStatement(query).setFetchSize(fetchSize));
    }

    public ResultSet execute(String query, Object... args) {
        return execute(FETCH, query, args);
    }

    public ResultSet execute(int fetchSize, String query, Object... args) {
        return execute(fetchSize, String.format(query, args));
    }

    ResultSet execute(StringBuilder query) {
        return execute(query.toString());
    }

    <T extends Exception> CassandraUtils check(Runnable runnable, Class<T> expectedClass, String expectedMessage) {
        try {
            runnable.run();
            fail(String.format("Should have produced %s with message '%s'",
                               expectedClass.getSimpleName(),
                               expectedMessage));
        } catch (Exception e) {
            assertEquals("Expected exception type is wrong", expectedClass, e.getClass());
            assertTrue("Expected exception message is wrong, expected: "+expectedMessage+ " produced:"+e.getMessage(), e.getMessage().contains(expectedMessage));
        }
        return this;
    }

    private CassandraUtils waitForIndexBuilt() {
        logger.debug("Waiting for the index to be created...");
        while (!isIndexBuilt()) {
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while waiting for index building", e);
            }
        }
        logger.debug("Index ready to rock!");
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
        if (clusteringOrderColumn != null) {
            sb.append(" WITH CLUSTERING ORDER BY(");
            sb.append(clusteringOrderColumn);
            sb.append(" ");
            sb.append(this.clusteringOrderAscending ? "ASC" : "DESC");
            sb.append(")");
        }
        execute(sb);
        return this;
    }

    public <T extends Exception> CassandraUtils createTable(Class<T> expectedClass, String expectedMessage) {
        return check(this::createTable, expectedClass, expectedMessage);
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
        Index index = index(keyspace, table, indexName).column(indexColumn)
                                                       .refreshSeconds(REFRESH)
                                                       .indexingThreads(THREADS)
                                                       .partitioner(partitioner);
        mappers.forEach(index::mapper);
        analyzers.forEach(index::analyzer);
        execute(index.build());

        return waitForIndexBuilt();
    }

    public <T extends Exception> CassandraUtils createIndex(Class<T> expectedClass, String expectedMessage) {
        return check(this::createIndex, expectedClass, expectedMessage);
    }

    public CassandraUtils dropIndex() {
        execute(new StringBuilder().append("DROP INDEX ").append(keyspace).append(".").append(indexName).append(";"));
        return this;
    }

    public CassandraUtilsSelect searchAllWithFiltering(int limit, String name, Object value) {
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

    public <T extends Exception> CassandraUtils insert(Class<T> expectedClass,
                                                       String expectedMessage,
                                                       final Map<String, String>... paramss) {
        return check(() -> insert(paramss), expectedClass, expectedMessage);
    }

    public final CassandraUtils insert(String names, Object... values) {
        return insert(names.split(","), values);
    }

    public CassandraUtils insert(String[] names, Object[] values) {
        execute(QueryBuilder.insertInto(keyspace, table).values(names, values));
        return this;
    }

    public CassandraUtils insert(String[] names, Iterable<Object[]> values) {
        Batch batch = QueryBuilder.unloggedBatch();
        for (Object[] vs : values) {
            batch.add(QueryBuilder.insertInto(keyspace, table).values(names, vs));
        }
        execute(batch);
        return this;
    }

    public <T extends Exception> CassandraUtils insert(String[] names,
                                                       Object[] values,
                                                       Class<T> expectedClass,
                                                       String expectedMessage) {
        return check(() -> insert(names, values), expectedClass, expectedMessage);
    }

    public CassandraUtils insert(String[] names, Object[] values, Integer ttl) {
        execute(QueryBuilder.insertInto(keyspace, table).values(names, values).using(QueryBuilder.ttl(ttl)));
        return this;
    }

    public Insert asInsert(String[] names, Object[] values) {
        return QueryBuilder.insertInto(keyspace, table).values(names, values);
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

    public CassandraUtilsSelect search() {
        return select().search();
    }

    public CassandraUtilsSelect searchAll() {
        return select().search().filter(all());
    }

    public CassandraUtilsSelect filter(Condition... conditions) {
        return select().filter(conditions);
    }

    public CassandraUtilsSelect query(Condition... conditions) {
        return select().query(conditions);
    }

    public CassandraUtilsSelect sort(SortField... fields) {
        return select().sort(fields);
    }

    public List<Row> searchWithPreparedStatement(Search search) {
        String query = useNewQuerySyntax
                       ? String.format("SELECT * FROM %s WHERE expr(%s,?) LIMIT %d", qualifiedTable, indexName, LIMIT)
                       : String.format("SELECT * FROM %s WHERE %s = ? LIMIT %d", qualifiedTable, indexColumn, LIMIT);
        final PreparedStatement stmt = prepare(query);
        BoundStatement b = stmt.bind();
        b.setString(0, search.build());
        return execute(b).all();
    }

    public CassandraUtils flush() {
        logger.debug("JMX: Flush");
        invokeJMXMethod("org.apache.cassandra.db:type=StorageService",
                        "forceKeyspaceFlush",
                        new Object[]{keyspace, new String[]{table}},
                        new String[]{String.class.getName(), String[].class.getName()});
        return this;
    }

    public CassandraUtils refresh() {
        logger.debug("JMX: Refresh");
        invokeJMXMethod(indexBean, "refresh", new Object[]{}, new String[]{});
        return this;
    }

    public CassandraUtils commit() {
        logger.debug("JMX: Commit");
        invokeJMXMethod(indexBean, "commit", new Object[]{}, new String[]{});
        return this;
    }

    public CassandraUtils compact(boolean splitOutput) {
        logger.debug("JMX: Compact");
        invokeJMXMethod("org.apache.cassandra.db:type=StorageService",
                        "forceKeyspaceCompaction",
                        new Object[]{splitOutput, keyspace, new String[]{table}},
                        new String[]{boolean.class.getName(),
                                     String.class.getName(),
                                     String[].class.getName()});
        return this;
    }

    public void checkNumDocsInIndex(Integer expectedNumDocs) {
        List<Long> numDocsInEachNode = getJMXAttribute(indexBean, "NumDocs");
        Long totalNumDocs = numDocsInEachNode.stream().reduce(0L, (l, r) -> l + r) / (long) REPLICATION;
        assertEquals("NumDocs in index is not correct", new Long(expectedNumDocs), totalNumDocs);
    }

    @SuppressWarnings("unchecked")
    private boolean isIndexBuilt() {
        String bean = String.format("org.apache.cassandra.db:type=%s,keyspace=%s,table=%s", "Tables", keyspace, table);
        List<List<String>> builtIndexes = getJMXAttribute(bean, "BuiltIndexes");
        return builtIndexes.stream().allMatch(l -> l.contains(indexName));
    }
}
