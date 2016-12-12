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

import com.stratio.cassandra.lucene.builder.index.Partitioner;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.Analyzer;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.SingleColumnMapper;

import java.util.*;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraConfig.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraUtilsBuilder {

    private final String name;
    private String table = TABLE;
    private String indexName = INDEX;
    private String indexColumn = COLUMN;
    private Boolean useNewQuerySyntax = USE_NEW_QUERY_SYNTAX;
    private Map<String, String> columns;
    private Map<String, Mapper> mappers;
    private Map<String, Analyzer> analyzers;
    private List<String> partitionKey;
    private List<String> clusteringKey;
    private String clusteringOrderColumn;
    private boolean clusteringOrderAscending;
    private Partitioner partitioner = PARTITIONER;

    private final Map<String, Map<String, String>> udts;

    CassandraUtilsBuilder(String name) {
        super();
        this.name = name;
        this.columns = new HashMap<>();
        this.mappers = new HashMap<>();
        this.analyzers = new HashMap<>();
        this.partitionKey = new ArrayList<>();
        this.clusteringKey = new ArrayList<>();
        this.udts = new LinkedHashMap<>();
    }

    public CassandraUtilsBuilder withTable(String table) {
        this.table = table;
        return this;
    }

    public CassandraUtilsBuilder withIndexName(String index) {
        this.indexName = index;
        return this;
    }

    public CassandraUtilsBuilder withIndexColumn(String indexedColumn) {
        this.indexColumn = indexedColumn;
        return this;
    }

    public CassandraUtilsBuilder withUseNewQuerySyntax(Boolean useNewQuerySyntax) {
        this.useNewQuerySyntax = useNewQuerySyntax;
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
        if (mapper != null) {
            mappers.put(name, mapper);
        }
        return this;
    }

    public CassandraUtilsBuilder withStaticColumn(String name, String type, boolean createMapper) {
        columns.put(name, type + " static");
        if (createMapper) {
            String baseType = type.replaceAll("(.*)(<|,)", "").replace(">", "");
            SingleColumnMapper<?> mapper = defaultMapper(baseType);
            mappers.put(name, mapper);
        }
        return this;
    }

    public CassandraUtilsBuilder withUDT(String column, String field, String type) {
        Map<String, String> udt = udts.get(column);
        if (udt == null) {
            udt = new HashMap<>();
            udts.put(column, udt);
        }
        udt.put(field, type);
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

    public CassandraUtilsBuilder withAnalyzer(String name, Analyzer analyzer) {
        analyzers.put(name, analyzer);
        return this;
    }

    public CassandraUtilsBuilder withClusteringOrder(String columnName, boolean ascending) {
        clusteringOrderColumn = columnName;
        clusteringOrderAscending = ascending;
        return this;
    }

    public CassandraUtilsBuilder withPartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    private SingleColumnMapper<?> defaultMapper(String name) {
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
                return dateMapper().pattern("yyyy/MM/dd HH:mm:ss.SSS");
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
                return null;
        }
    }

    public CassandraUtils build() {
        String keyspace = name + "_" + Math.abs(new Random().nextLong());
        return new CassandraUtils(keyspace,
                                  table,
                                  indexName,
                                  indexColumn,
                                  useNewQuerySyntax,
                                  columns,
                                  mappers,
                                  analyzers,
                                  partitionKey,
                                  clusteringKey,
                                  udts,
                                  clusteringOrderColumn,
                                  clusteringOrderAscending,
                                  partitioner);
    }
}
