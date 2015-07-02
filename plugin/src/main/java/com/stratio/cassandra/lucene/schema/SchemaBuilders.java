package com.stratio.cassandra.lucene.schema;

import com.stratio.cassandra.lucene.schema.analysis.AnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.analysis.ClasspathAnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.analysis.SnowballAnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper;
import com.stratio.cassandra.lucene.schema.mapping.builder.*;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.LinkedHashMap;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SchemaBuilders {

    public static SchemaBuilder schema() {
        return new SchemaBuilder(null,
                                 new LinkedHashMap<String, AnalyzerBuilder>(),
                                 new LinkedHashMap<String, MapperBuilder>());
    }

    public static BigDecimalMapperBuilder bigDecimalMapper() {
        return new BigDecimalMapperBuilder();
    }

    public static BigIntegerMapperBuilder bigIntegerMapper() {
        return new BigIntegerMapperBuilder();
    }

    public static BitemporalMapperBuilder bitemporalMapper(String vt_from, String vt_to, String tt_from, String tt_to) {
        return new BitemporalMapperBuilder(vt_from,vt_to,tt_from,tt_to);
    }

    public static BlobMapperBuilder blobMapper() {
        return new BlobMapperBuilder();
    }

    public static BooleanMapperBuilder booleanMapper() {
        return new BooleanMapperBuilder();
    }

    public static DateMapperBuilder dateMapper() {
        return new DateMapperBuilder();
    }

    public static DateRangeMapperBuilder dateRangeMapper(String start, String stop) {
        return new DateRangeMapperBuilder(start, stop);
    }

    public static DoubleMapperBuilder doubleMapper() {
        return new DoubleMapperBuilder();
    }

    public static FloatMapperBuilder floatMapper() {
        return new FloatMapperBuilder();
    }

    public static GeoPointMapperBuilder geoPointMapper(String latitude, String longitude) {
        return new GeoPointMapperBuilder(latitude, longitude);
    }

    public static InetMapperBuilder inetMapper() {
        return new InetMapperBuilder();
    }

    public static IntegerMapperBuilder integerMapper() {
        return new IntegerMapperBuilder();
    }

    public static LongMapperBuilder longMapper() {
        return new LongMapperBuilder();
    }

    public static StringMapperBuilder stringMapper() {
        return new StringMapperBuilder();
    }

    public static TextMapperBuilder textMapper() {
        return new TextMapperBuilder();
    }

    public static UUIDMapperBuilder UUIDMapper() {
        return new UUIDMapperBuilder();
    }

    public static ClasspathAnalyzerBuilder classpathAnalyzer(String className) {
        return new ClasspathAnalyzerBuilder(className);
    }

    public static SnowballAnalyzerBuilder snowballAnalyzer(String language, String stopwords) {
        return new SnowballAnalyzerBuilder(language, stopwords);
    }

}
