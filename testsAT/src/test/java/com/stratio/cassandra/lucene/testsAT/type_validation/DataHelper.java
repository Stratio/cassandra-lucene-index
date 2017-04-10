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
    public static final Map<String, Set<String>> singleColumnMappersAcceptedTypes = new HashMap<>();

    static {
        singleColumnMappersAcceptedTypes.put("big_decimal", Sets.newHashSet("ascii", "bigint", "decimal", "double", "float", "int", "smallint", "text", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put("big_integer", Sets.newHashSet("ascii", "bigint", "int", "smallint", "text", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put("blob", Sets.newHashSet("ascii", "blob", "text", "varchar"));
        singleColumnMappersAcceptedTypes.put("boolean", Sets.newHashSet("ascii", "boolean", "text", "varchar"));
        singleColumnMappersAcceptedTypes.put("date", Sets.newHashSet("ascii", "bigint", "date", "int", "text", "timestamp", "timeuuid", "uuid", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put("double", Sets.newHashSet("ascii", "bigint", "decimal", "double", "float", "int", "smallint", "text", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put("float", Sets.newHashSet("ascii", "bigint", "decimal", "double", "float", "int", "smallint", "text", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put("geo_shape", Sets.newHashSet("ascii", "text", "varchar"));
        singleColumnMappersAcceptedTypes.put("inet", Sets.newHashSet("ascii", "inet", "text", "varchar"));
        singleColumnMappersAcceptedTypes.put("integer", Sets.newHashSet("ascii", "bigint", "date", "decimal", "double", "float", "int", "smallint", "text", "timestamp", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put("long", Sets.newHashSet("ascii", "bigint", "date", "decimal", "double", "float", "int", "smallint", "text", "timestamp", "tinyint", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put("string", Sets.newHashSet("ascii", "bigint", "boolean", "decimal", "double", "float", "inet", "int", "smallint", "text", "timeuuid", "tinyint", "uuid", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put("text", Sets.newHashSet("ascii", "bigint", "boolean", "decimal", "double", "float", "inet", "int", "smallint", "text", "timeuuid", "tinyint", "uuid", "varchar", "varint"));
        singleColumnMappersAcceptedTypes.put("uuid", Sets.newHashSet("ascii", "text", "timeuuid", "uuid", "varchar"));
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

    protected static final Map<String, String> multipleColumnMapperInvalidColumnName = new HashMap<>();

    static {
        multipleColumnMapperInvalidColumnName.put("DateRangeMapper", "from_");
        multipleColumnMapperInvalidColumnName.put("GeoPointMapper", "latitude");
        multipleColumnMapperInvalidColumnName.put("BitemporalMapper", "vt_from");
    }

    protected static final Map<String, String> typePackageByName =  new LinkedHashMap<>();

    static {
        typePackageByName.put("bigint","org.apache.cassandra.db.marshal.LongType");
        typePackageByName.put("varint","org.apache.cassandra.db.marshal.IntegerType");
        typePackageByName.put("tinyint","org.apache.cassandra.db.marshal.ByteType");
        typePackageByName.put("smallint","org.apache.cassandra.db.marshal.ShortType");
        typePackageByName.put("int","org.apache.cassandra.db.marshal.Int32Type");
        typePackageByName.put("ascii","org.apache.cassandra.db.marshal.AsciiType");
        typePackageByName.put("boolean","org.apache.cassandra.db.marshal.BooleanType");
        typePackageByName.put("blob","org.apache.cassandra.db.marshal.BytesType");
        typePackageByName.put("decimal","org.apache.cassandra.db.marshal.DecimalType");
        typePackageByName.put("double","org.apache.cassandra.db.marshal.DoubleType");
        typePackageByName.put("float","org.apache.cassandra.db.marshal.FloatType");
        typePackageByName.put("inet","org.apache.cassandra.db.marshal.InetAddressType");
        typePackageByName.put("timeuuid","org.apache.cassandra.db.marshal.TimeUUIDType");
        typePackageByName.put("uuid","org.apache.cassandra.db.marshal.UUIDType");
        typePackageByName.put("date","org.apache.cassandra.db.marshal.SimpleDateType");
        typePackageByName.put("timestamp","org.apache.cassandra.db.marshal.TimestampType");
        typePackageByName.put("time","org.apache.cassandra.db.marshal.TimeType");
        typePackageByName.put("text","org.apache.cassandra.db.marshal.UTF8Type");
        typePackageByName.put("varchar","org.apache.cassandra.db.marshal.UTF8Type");
        typePackageByName.put("list","org.apache.cassandra.db.marshal.ListType");
        typePackageByName.put("set","org.apache.cassandra.db.marshal.SetType");
        typePackageByName.put("map","org.apache.cassandra.db.marshal.MapType");
    }

    public static final Map<String, Mapper> mapperByName = new LinkedHashMap<>();

    static {
        mapperByName.put("big_decimal", bigDecimalMapper());
        mapperByName.put("big_integer", bigIntegerMapper());
        mapperByName.put("blob", blobMapper());
        mapperByName.put("boolean", booleanMapper());
        mapperByName.put("date", dateMapper());
        mapperByName.put("double", doubleMapper());
        mapperByName.put("float", floatMapper());
        mapperByName.put("geo_shape", geoShapeMapper());
        mapperByName.put("inet", inetMapper());
        mapperByName.put("integer", integerMapper());
        mapperByName.put("long", longMapper());
        mapperByName.put("string", stringMapper());
        mapperByName.put("text", textMapper());
        mapperByName.put("uuid", uuidMapper());
    }

    protected static final Map<String,String> unsupportedTypes= new HashMap<>();

    static {
        unsupportedTypes.put("time","org.apache.cassandra.db.marshal.TimeType");
    }

    protected static String buildIndexMessage(String mapperName, String cqlType) {
        return buildIndexMessage(mapperName, cqlType,"column");
    }

    private static boolean isComplexType(String type) {
        return type.contains("<");

    }

    private static String getTypePackage(String cqlType) {
        if (isComplexType(cqlType)) {
            String complexType = cqlType;
            for (String typ : typePackageByName.keySet()) {
                complexType = complexType.replace(typ, typePackageByName.get(typ));
            }
            complexType = complexType.replace("<", "(");
            return complexType.replace(">", ")");
        } else {
            return typePackageByName.get(cqlType);
        }
    }

    public static String buildIndexMessage(String mapperName, String cqlType, String column) {
        StringBuilder sb = new StringBuilder().append("'schema' is invalid : ");
        String typePackage = unsupportedTypes.get(cqlType);
        if (typePackage != null) {
            sb.append("Unsupported Cassandra data type: class ");
            sb.append(typePackage);
        } else {
            //'schema' is invalid : Type 'org.apache.cassandra.db.marshal.SimpleDateType' in column 'column' is not supported by mapper 'column'
            sb.append("Type '");
            sb.append(getTypePackage(cqlType));
            sb.append("' in column '");
            sb.append(column);
            sb.append("' is not supported by mapper '");
            sb.append(mapperName);
            sb.append("'");
        }

        return sb.toString();
    }

    public static String listComposedType(String type) {
        return new StringBuilder().append("list<").append(type).append(">").toString();
    }
    public static String setComposedType(String type) {
        return new StringBuilder().append("set<").append(type).append(">").toString();
    }
    public static String mapComposedType(String type) {
        return new StringBuilder().append("map<bigint,").append(type).append(">").toString();
    }
    public static String buildTableName(String mapperName, String cqlType) {
        return (mapperName + "_" + cqlType.replace("<","_").replace(">","_").replace(",","_")).toLowerCase();
    }
}

