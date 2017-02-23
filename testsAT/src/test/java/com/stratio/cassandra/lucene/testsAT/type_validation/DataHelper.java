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
    protected static final Set<String> ALL_CQL_TYPES = new HashSet<>(Arrays.asList(CQL_TYPES));
    protected static final Map<Mapper, Set<String>> singleColumnMappersAcceptedTypes = new HashMap<>();

    static {
        singleColumnMappersAcceptedTypes.put(bigDecimalMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "decimal", "double", "float", "int", "smallint", "text", "tinyint", "varchar", "varint" })));
        singleColumnMappersAcceptedTypes.put(bigIntegerMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "int", "smallint", "text", "tinyint", "varchar", "varint" })));
        singleColumnMappersAcceptedTypes.put(blobMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "blob", "text", "varchar" })));
        singleColumnMappersAcceptedTypes.put(booleanMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "boolean ", "text", "varchar" })));
        singleColumnMappersAcceptedTypes.put(dateMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "date", "int", "text", "timestamp", "timeuuid", "varchar", "varint" })));
        singleColumnMappersAcceptedTypes.put(doubleMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "decimal", "double", "float", "int", "smallint", "text", "tinyint", "varchar", "varint" })));
        singleColumnMappersAcceptedTypes.put(floatMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "decimal", "double", "float", "int", "smallint", "tinyint", "varchar", "varint" })));
        singleColumnMappersAcceptedTypes.put(geoShapeMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "text", "varchar" })));
        singleColumnMappersAcceptedTypes.put(inetMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "inet", "text", "varchar" })));
        singleColumnMappersAcceptedTypes.put(integerMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "date", "decimal", "double", "float", "int", "smallint", "text", "timestamp", "tinyint", "varchar", "varint" })));
        singleColumnMappersAcceptedTypes.put(longMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "date", "decimal", "double", "float", "int", "smallint", "text", "timestamp", "tinyint", "varchar", "varint" })));
        singleColumnMappersAcceptedTypes.put(stringMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "blob", "boolean", "double", "float", "inet", "int", "smallint", "text", "timestamp", "timeuuid", "tinyint", "uuid", "varchar", "varint" })));
        singleColumnMappersAcceptedTypes.put(textMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "blob", "boolean", "double", "float", "inet", "int", "smallint", "text", "timestamp", "timeuuid", "tinyint", "uuid", "varchar", "varint" })));
        singleColumnMappersAcceptedTypes.put(uuidMapper(), new HashSet<>(Arrays.asList(new String[]{"ascii", "text", "timeuuid", "uuid", "varchar" })));
    }

    //"list", "map", "set",

    protected static final Map<Mapper, Set<String>> multipleColumnMappersAcceptedTypes = new HashMap<>();

    static {
        multipleColumnMappersAcceptedTypes.put(bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to"), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "date", "int", "text", "timestamp", "timeuuid", "varchar", "varint" })));
        multipleColumnMappersAcceptedTypes.put(dateRangeMapper("from_", "to_"), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "date", "int", "text", "timestamp", "timeuuid", "varchar", "varint" })));
        multipleColumnMappersAcceptedTypes.put(geoPointMapper("latitude", "longitude"), new HashSet<>(Arrays.asList(new String[]{"ascii", "bigint", "decimal", "double", "float", "int", "smallint", "text", "timestamp", "varchar", "varint" })));
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
}
