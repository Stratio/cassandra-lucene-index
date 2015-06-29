package com.stratio.cassandra.lucene.schema;

import com.stratio.cassandra.lucene.schema.analysis.AnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.analysis.ClasspathAnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import com.stratio.cassandra.lucene.schema.analysis.SnowballAnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.schema.mapping.builder.*;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.lucene.analysis.Analyzer;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SchemaBuilder {

    @JsonProperty("default_analyzer")
    private String defaultAnalyzerName;

    @JsonProperty("analyzers")
    private final Map<String, AnalyzerBuilder> analyzerBuilders;

    @JsonProperty("fields")
    private final Map<String, MapperBuilder> mapperBuilders;

    @JsonCreator
    SchemaBuilder(@JsonProperty("default_analyzer") String defaultAnalyzerName,
                  @JsonProperty("analyzers") Map<String, AnalyzerBuilder> analyzerBuilders,
                  @JsonProperty("fields") Map<String, MapperBuilder> mapperBuilders) {
        this.defaultAnalyzerName = defaultAnalyzerName;
        this.analyzerBuilders = analyzerBuilders != null ? analyzerBuilders : new HashMap<String, AnalyzerBuilder>();
        this.mapperBuilders = mapperBuilders != null ? mapperBuilders : new HashMap<String, MapperBuilder>();
    }

    public SchemaBuilder defaultAnalyzer(String name) {
        defaultAnalyzerName = name;
        return this;
    }

    public SchemaBuilder analyzer(String name, AnalyzerBuilder analyzer) {
        analyzerBuilders.put(name, analyzer);
        return this;
    }

    public SchemaBuilder mapper(String name, MapperBuilder mapper) {
        mapperBuilders.put(name, mapper);
        return this;
    }

    public Schema build() {

        Map<String, Mapper> mappers = new HashMap<>(mapperBuilders.size());
        for (Map.Entry<String, MapperBuilder> entry : mapperBuilders.entrySet()) {
            String name = entry.getKey();
            MapperBuilder builder = entry.getValue();
            Mapper mapper = builder.build(name);
            mappers.put(name, mapper);
        }

        Map<String, Analyzer> analyzers = new HashMap<>();
        for (Map.Entry<String, AnalyzerBuilder> entry : analyzerBuilders.entrySet()) {
            String name = entry.getKey();
            Analyzer analyzer = entry.getValue().analyzer();
            analyzers.put(name, analyzer);
        }

        Analyzer defaultAnalyzer;
        if (defaultAnalyzerName == null) {
            defaultAnalyzer = PreBuiltAnalyzers.DEFAULT.get();
        } else {
            defaultAnalyzer = analyzers.get(defaultAnalyzerName);
            if (defaultAnalyzer == null) {
                defaultAnalyzer = PreBuiltAnalyzers.get(defaultAnalyzerName);
                if (defaultAnalyzer == null) {
                    try {
                        defaultAnalyzer = (new ClasspathAnalyzerBuilder(defaultAnalyzerName)).analyzer();
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Not found analyzer: " + defaultAnalyzerName);
                    }
                }
                analyzers.put(defaultAnalyzerName, defaultAnalyzer);
            }
        }
        return new Schema(defaultAnalyzer, mappers, analyzers);
    }

    public String toJson() throws IOException {
        return JsonSerializer.toString(this);
    }

    /**
     * Returns the {@link Schema} contained in the specified JSON {@code String}.
     *
     * @param json A {@code String} containing the JSON representation of the {@link Schema} to be parsed.
     * @return The {@link Schema} contained in the specified JSON {@code String}.
     */
    public static SchemaBuilder fromJson(String json) throws IOException {
        return JsonSerializer.fromString(json, SchemaBuilder.class);
    }

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
