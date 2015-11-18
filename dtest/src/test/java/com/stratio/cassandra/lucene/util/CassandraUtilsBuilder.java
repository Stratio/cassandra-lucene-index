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

import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.SingleColumnMapper;

import java.util.*;

import static com.stratio.cassandra.lucene.TestingConstants.*;
import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraUtilsBuilder {

    private final String name;
    private String host = CASSANDRA_LOCALHOST_CONSTANT;
    private String table = TABLE_NAME_CONSTANT;
    private String index = INDEX_NAME_CONSTANT;
    private Integer fetchSize = FETCH_SIZE;
    private Map<String, String> columns;
    private Map<String, Mapper> mappers;
    private List<String> partitionKey;
    private List<String> clusteringKey;
    private String indexColumn = INDEX_COLUMN_CONSTANT;

    CassandraUtilsBuilder(String name) {
        super();
        this.name = name;
        this.columns = new HashMap<>();
        this.mappers = new HashMap<>();
        this.partitionKey = new ArrayList<>();
        this.clusteringKey = new ArrayList<>();
    }

    public CassandraUtilsBuilder withHost(String host) {
        this.host = host;
        return this;
    }

    public CassandraUtilsBuilder withTable(String table) {
        this.table = table;
        return this;
    }

    public CassandraUtilsBuilder withIndex(String index) {
        this.index = index;
        return this;
    }

    public CassandraUtilsBuilder withIndexColumn(String indexColumn) {
        this.indexColumn = indexColumn;
        return this;
    }

    public CassandraUtilsBuilder withFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public CassandraUtilsBuilder withColumn(String name, String type, Mapper mapper) {
        columns.put(name, type);
        if (mapper != null) {
            mappers.put(name, mapper);
        }
        return this;
    }

    public CassandraUtilsBuilder withColumn(String name, String type) {
        columns.put(name, type);
        String baseType = type.replaceAll("(.*)(<|,)", "").replace(">", "");
        SingleColumnMapper<?> mapper = defaultMapper(baseType);
        if (baseType.equals(type)) {
            mapper.sorted(true);
        }
        mappers.put(name, mapper);
        return this;
    }

    public CassandraUtilsBuilder withPartitionKey(String... columns) {
        partitionKey.addAll(Arrays.asList(columns));
        return this;
    }

    public CassandraUtilsBuilder withClusteringKey(String... columns) {
        clusteringKey.addAll(Arrays.asList(columns));
        return this;
    }

    public CassandraUtilsBuilder withMapper(String name, Mapper mapper) {
        mappers.put(name, mapper);
        return this;
    }

    public SingleColumnMapper<?> defaultMapper(String name) {
        switch (name) {
            case "ascii":
                return stringMapper();
            case "bigint":
                return longMapper();
            case "blob":
                return blobMapper();
            case "boolean":
                return booleanMapper();
            case "counter":
                return longMapper();
            case "decimal":
                return bigDecimalMapper().integerDigits(10).decimalDigits(10);
            case "double":
                return doubleMapper();
            case "float":
                return floatMapper();
            case "inet":
                return inetMapper();
            case "int":
                return integerMapper();
            case "smallint":
                return integerMapper();
            case "text":
                return textMapper();
            case "timestamp":
                return dateMapper().pattern("yyyy/MM/dd");
            case "timeuuid":
                return uuidMapper();
            case "tinyint":
                return integerMapper();
            case "uuid":
                return uuidMapper();
            case "varchar":
                return stringMapper();
            case "varint":
                return bigIntegerMapper().digits(10);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public CassandraUtils build() {
        String keyspace = name + "_" + Math.abs(new Random().nextLong());
        return new CassandraUtils(host,
                                  keyspace,
                                  table,
                                  index,
                                  fetchSize,
                                  columns,
                                  mappers,
                                  partitionKey,
                                  clusteringKey,
                                  indexColumn);
    }
}
