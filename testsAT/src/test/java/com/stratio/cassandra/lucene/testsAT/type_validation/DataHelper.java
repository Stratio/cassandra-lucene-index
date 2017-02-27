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
package com.stratio.cassandra.lucene.testsAT.type_validation;

import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;

import java.util.*;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.builder.Builder.uuidMapper;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public final class DataHelper {
    protected static final String KEYSPACE_NAME= "cql_types_validation";
    protected static final String[] CQL_TYPES = new String[]{"ascii", "bigint", "blob", "boolean", "date", "decimal", "double", "float", "inet", "int", "smallint", "text", "time", "timestamp", "timeuuid", "tinyint", "uuid", "varchar", "varint" };
    protected static final Set<String> ALL_CQL_TYPES = Sets.newHashSet(CQL_TYPES);
    protected static final Map<Mapper, Set<String>> singleColumnMappersAcceptedTypes = new HashMap<>();

    static {
        singleColumnMappersAcceptedTypes.put(bigDecimalMapper(), Sets.newHashSet("ascii", "bigint", "decimal", "double", "float", "int", "smallint", "text", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put(bigIntegerMapper(), Sets.newHashSet("ascii", "bigint", "int", "smallint", "text", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put(blobMapper(), Sets.newHashSet("ascii", "blob", "text", "varchar"));
        singleColumnMappersAcceptedTypes.put(booleanMapper(), Sets.newHashSet("ascii", "boolean", "text", "varchar"));
        singleColumnMappersAcceptedTypes.put(dateMapper(), Sets.newHashSet("ascii", "bigint", "date", "int", "text", "timestamp", "timeuuid", "uuid", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put(doubleMapper(), Sets.newHashSet("ascii", "bigint", "decimal", "double", "float", "int", "smallint", "text", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put(floatMapper(), Sets.newHashSet("ascii", "bigint", "decimal", "double", "float", "int", "smallint", "text", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put(geoShapeMapper(), Sets.newHashSet("ascii", "text", "varchar"));
        singleColumnMappersAcceptedTypes.put(inetMapper(), Sets.newHashSet("ascii", "inet", "text", "varchar"));
        singleColumnMappersAcceptedTypes.put(integerMapper(), Sets.newHashSet("ascii", "bigint", "date", "decimal", "double", "float", "int", "smallint", "text", "timestamp", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put(longMapper(), Sets.newHashSet("ascii", "bigint", "date", "decimal", "double", "float", "int", "smallint", "text", "timestamp", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put(stringMapper(), Sets.newHashSet("ascii", "bigint", "boolean", "decimal", "double", "float", "inet", "int", "smallint", "text", "timeuuid", "tinyint", "uuid", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put(textMapper(), Sets.newHashSet("ascii", "bigint", "boolean", "decimal", "double", "float", "inet", "int", "smallint", "text", "timeuuid", "tinyint", "uuid", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put(uuidMapper(), Sets.newHashSet("ascii", "text", "timeuuid", "uuid", "varchar"));
    }

    //"list", "map", "set",

    protected static final Map<Mapper, Set<String>> multipleColumnMappersAcceptedTypes = new HashMap<>();

    static {
        multipleColumnMappersAcceptedTypes.put(bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to"), Sets.newHashSet("ascii", "bigint", "date", "int", "text", "timestamp", "timeuuid", "uuid", "varchar", "varint"));
        multipleColumnMappersAcceptedTypes.put(dateRangeMapper("from_", "to_"), Sets.newHashSet("ascii", "bigint", "date", "int", "text", "timestamp", "timeuuid", "uuid", "varchar", "varint"));
        multipleColumnMappersAcceptedTypes.put(geoPointMapper("latitude", "longitude"), Sets.newHashSet("ascii", "bigint", "decimal", "double", "float", "int", "smallint", "text", "varchar", "varint"));
    }

    protected static final Map<String, Set<String>> multipleColumnMapperRequiredColumnNames = new HashMap<>();

    static {
        multipleColumnMapperRequiredColumnNames.put(bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").toString(), Sets.newHashSet("vt_from", "vt_to", "tt_from", "tt_to"));
        multipleColumnMapperRequiredColumnNames.put(dateRangeMapper("from_", "to_").toString(),  Sets.newHashSet("from_", "to_"));
        multipleColumnMapperRequiredColumnNames.put(geoPointMapper("latitude", "longitude").toString(), Sets.newHashSet("latitude", "longitude"));
    }


    protected static String buildIndexMessage(Mapper mapper, String cqlType) {
        return new StringBuilder().toString();
    }

    protected static String listComposedType(String type) {
        return new StringBuilder().append("list<").append(type).append(">").toString();
    }
    protected static String setComposedType(String type) {
        return new StringBuilder().append("set<").append(type).append(">").toString();
    }
    protected static String mapComposedType(String type) {
        return new StringBuilder().append("map<bigint,").append(type).append(">").toString();
    }
    protected static String buildTableName(String mapperName, String cqlType) {
        return (mapperName + "_" + cqlType.replace("<","_").replace(">","_").replace(",","_")).toLowerCase();
    }
}
