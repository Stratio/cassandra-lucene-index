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

package com.stratio.cassandra.lucene.builder.index;

import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.index.schema.Schema;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.Analyzer;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;

/**
 * A Lucene index definition.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Index extends Builder {

    private Schema schema;
    private String keyspace;
    private String table;
    private String column;
    private String name;
    private Number refreshSeconds;
    private String directoryPath;
    private Integer ramBufferMb;
    private Integer maxMergeMb;
    private Integer maxCachedMb;
    private Integer indexingThreads;
    private Integer indexingQueuesSize;
    private String excludedDataCenters;

    /**
     * Builds a new {@link Index} creation statement for the specified table and column.
     *
     * @param table  The table name.
     * @param column THe indexed column name.
     */
    public Index(String table, String column) {
        this.schema = new Schema();
        this.table = table;
        this.column = column;
    }

    /**
     * Sets the name keyspace name.
     *
     * @param keyspace The keyspace name.
     * @return This.
     */
    public Index keyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Sets the name of the indexed column.
     *
     * @param name The name of the indexed column.
     * @return This.
     */
    public Index name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Sets the index searcher refresh period.
     *
     * @param refreshSeconds The refresh seconds.
     * @return This.
     */
    public Index refreshSeconds(Number refreshSeconds) {
        this.refreshSeconds = refreshSeconds;
        return this;
    }

    /**
     * Sets the path of the Lucene directory files.
     *
     * @param directoryPath The path of the Lucene directory files.
     * @return This.
     */
    public Index directoryPath(String directoryPath) {
        this.directoryPath = directoryPath;
        return this;
    }

    /**
     * Sets the Lucene's RAM buffer size in MBs.
     *
     * @param ramBufferMb The Lucene's RAM buffer size in MBs.
     * @return This.
     */
    public Index ramBufferMb(Integer ramBufferMb) {
        this.ramBufferMb = ramBufferMb;
        return this;
    }

    /**
     * Sets the Lucene's max merge MBs.
     *
     * @param maxMergeMb The Lucene's max merge MBs.
     * @return This.
     */
    public Index maxMergeMb(Integer maxMergeMb) {
        this.maxMergeMb = maxMergeMb;
        return this;
    }

    /**
     * Sets the Lucene's max cached MBs.
     *
     * @param maxCachedMb The Lucene's max cached MBs.
     * @return This.
     */
    public Index maxCachedMb(Integer maxCachedMb) {
        this.maxCachedMb = maxCachedMb;
        return this;
    }

    /**
     * Sets the number of asynchronous indexing threads, where {@code 0} means synchronous indexing.
     *
     * @param indexingThreads The number of asynchronous indexing threads.
     * @return This.
     */
    public Index indexingThreads(Integer indexingThreads) {
        this.indexingThreads = indexingThreads;
        return this;
    }

    /**
     * Sets the max number of queued documents per asynchronous indexing thread.
     *
     * @param indexingQueuesSize The max number of queued documents per asynchronous indexing thread.
     * @return This.
     */
    public Index indexingQueuesSize(Integer indexingQueuesSize) {
        this.indexingQueuesSize = indexingQueuesSize;
        return this;
    }

    /**
     * Sets the list of excluded data centers.
     *
     * @param excludedDataCenters The list of excluded data centers.
     * @return This.
     */
    public Index excludedDataCenters(String excludedDataCenters) {
        this.excludedDataCenters = excludedDataCenters;
        return this;
    }

    /**
     * Sets the name of the default {@link Analyzer}.
     *
     * @param name The name of the default {@link Analyzer}.
     * @return This.
     */
    public Index defaultAnalyzer(String name) {
        schema.defaultAnalyzer(name);
        return this;
    }

    /**
     * Adds a new {@link Analyzer}.
     *
     * @param name     The name of the {@link Analyzer} to be added.
     * @param analyzer The builder of the {@link Analyzer} to be added.
     * @return This.
     */
    public Index analyzer(String name, Analyzer analyzer) {
        schema.analyzer(name, analyzer);
        return this;
    }

    /**
     * Adds a new {@link Mapper}.
     *
     * @param field  The name of the {@link Mapper} to be added.
     * @param mapper The builder of the {@link Mapper} to be added.
     * @return This.
     */
    public Index mapper(String field, Mapper mapper) {
        schema.mapper(field, mapper);
        return this;
    }

    /**
     * Sets the {@link Schema}.
     *
     * @param schema A {@link Schema}.
     * @return This.
     */
    public Index schema(Schema schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public String build() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE CUSTOM INDEX ");
        if (name != null) {
            sb.append(name).append(" ");
        }
        String fullTable = keyspace == null ? table : keyspace + "." + table;
        sb.append(String.format("ON %s (%s) ", fullTable, column));
        sb.append("USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {");
        option(sb, "refresh_seconds", refreshSeconds);
        option(sb, "directory_path", directoryPath);
        option(sb, "ram_buffer_mb", ramBufferMb);
        option(sb, "max_merge_mb", maxMergeMb);
        option(sb, "max_cached_mb", maxCachedMb);
        option(sb, "indexing_threads", indexingThreads);
        option(sb, "indexing_queues_size", indexingQueuesSize);
        option(sb, "excluded_data_centers", excludedDataCenters);
        sb.append(String.format("'schema':'%s'}", schema));
        return sb.toString();
    }

    private void option(StringBuilder sb, String name, Object value) {
        if (value != null) {
            sb.append(String.format("'%s':'%s',", name, value));
        }
    }

}
