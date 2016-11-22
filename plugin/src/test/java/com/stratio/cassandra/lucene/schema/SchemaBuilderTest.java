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
package com.stratio.cassandra.lucene.schema;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.analysis.SnowballAnalyzerBuilder.SnowballAnalyzer;
import com.stratio.cassandra.lucene.schema.analysis.StandardAnalyzers;
import com.stratio.cassandra.lucene.schema.mapping.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SchemaBuilderTest {

    @Test
    public void testBuild() throws Exception {
        Schema schema = schema().defaultAnalyzer("custom")
                                .analyzer("custom", classpathAnalyzer("org.apache.lucene.analysis.en.EnglishAnalyzer"))
                                .analyzer("snowball", snowballAnalyzer("English", "the,at"))
                                .mapper("blob", blobMapper())
                                .mapper("bool", booleanMapper())
                                .mapper("date", dateMapper())
                                .mapper("double", doubleMapper())
                                .mapper("float", floatMapper())
                                .mapper("inet", inetMapper())
                                .mapper("string", stringMapper())
                                .mapper("text", textMapper().analyzer("snowball"))
                                .mapper("uuid", uuidMapper())
                                .build();
        assertEquals("Failed schema building", EnglishAnalyzer.class, schema.defaultAnalyzer.getClass());
        assertEquals("Failed schema building", BlobMapper.class, schema.mapper("blob").getClass());
        assertEquals("Failed schema building", BooleanMapper.class, schema.mapper("bool").getClass());
        assertEquals("Failed schema building", DateMapper.class, schema.mapper("date").getClass());
        assertEquals("Failed schema building", InetMapper.class, schema.mapper("inet").getClass());
        assertEquals("Failed schema building", StringMapper.class, schema.mapper("string").getClass());
        assertEquals("Failed schema building", TextMapper.class, schema.mapper("text").getClass());
        assertEquals("Failed schema building", SnowballAnalyzer.class, schema.analyzer("text").getClass());
        assertEquals("Failed schema building", UUIDMapper.class, schema.mapper("uuid").getClass());
    }

    @Test
    public void testBuildNumeric() throws Exception {
        Schema schema = schema().defaultAnalyzer("custom")
                                .analyzer("custom", classpathAnalyzer("org.apache.lucene.analysis.en.EnglishAnalyzer"))
                                .analyzer("snowball", snowballAnalyzer("English", "the,at"))
                                .mapper("big_int", bigIntegerMapper().digits(10))
                                .mapper("big_dec", bigDecimalMapper())
                                .mapper("double", doubleMapper())
                                .mapper("float", floatMapper())
                                .mapper("int", integerMapper().boost(0.3f))
                                .mapper("long", longMapper())
                                .build();
        assertEquals("Failed schema building", EnglishAnalyzer.class, schema.defaultAnalyzer.getClass());
        assertEquals("Failed schema building", BigIntegerMapper.class, schema.mapper("big_int").getClass());
        assertEquals("Failed schema building", BigDecimalMapper.class, schema.mapper("big_dec").getClass());
        assertEquals("Failed schema building", DoubleMapper.class, schema.mapper("double").getClass());
        assertEquals("Failed schema building", FloatMapper.class, schema.mapper("float").getClass());
        assertEquals("Failed schema building", IntegerMapper.class, schema.mapper("int").getClass());
        assertEquals("Failed schema building", LongMapper.class, schema.mapper("long").getClass());
    }

    @Test
    public void testBuildNulls() throws Exception {
        new SchemaBuilder(null, null, null);
    }

    @Test
    public void testBuildComplex() throws Exception {
        Schema schema = schema().defaultAnalyzer("custom")
                                .analyzer("custom", classpathAnalyzer("org.apache.lucene.analysis.en.EnglishAnalyzer"))
                                .analyzer("snowball", snowballAnalyzer("English", "the,at"))
                                .mapper("bitemporal", bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to"))
                                .mapper("date_range", dateRangeMapper("from", "to"))
                                .mapper("geo", geoPointMapper("lat", "lon"))
                                .build();
        assertEquals("Failed schema building", EnglishAnalyzer.class, schema.defaultAnalyzer.getClass());
        assertEquals("Failed schema building", BitemporalMapper.class, schema.mapper("bitemporal").getClass());
        assertEquals("Failed schema building", DateRangeMapper.class, schema.mapper("date_range").getClass());
        assertEquals("Failed schema building", GeoPointMapper.class, schema.mapper("geo").getClass());
    }

    @Test
    public void testToJson() throws IOException {
        String json = schema().defaultAnalyzer("custom")
                              .analyzer("custom", classpathAnalyzer("org.apache.lucene.analysis.en.EnglishAnalyzer"))
                              .analyzer("snowball", snowballAnalyzer("English", "the,at"))
                              .mapper("big_int", bigIntegerMapper().digits(10))
                              .mapper("big_dec", bigDecimalMapper())
                              .mapper("bitemporal", bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo"))
                              .mapper("blob", blobMapper())
                              .mapper("bool", booleanMapper())
                              .mapper("date", dateMapper())
                              .mapper("date_range", dateRangeMapper("from", "to"))
                              .mapper("double", doubleMapper())
                              .mapper("float", floatMapper())
                              .mapper("geo", geoPointMapper("lat", "lon"))
                              .mapper("inet", inetMapper())
                              .mapper("int", integerMapper().boost(0.3f))
                              .mapper("long", longMapper())
                              .mapper("string", stringMapper())
                              .mapper("text", textMapper())
                              .mapper("uuid", uuidMapper())
                              .toJson();
        String expectedJson = "{default_analyzer:\"custom\"," +
                              "analyzers:{" +
                              "custom:{type:\"classpath\",class:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}," +
                              "snowball:{type:\"snowball\",language:\"English\",stopwords:\"the,at\"}}," +
                              "fields:" +
                              "{big_int:{type:\"bigint\",digits:10}," +
                              "big_dec:{type:\"bigdec\"}," +
                              "bitemporal:{type:\"bitemporal\"," +
                              "vt_from:\"vtFrom\",vt_to:\"vtTo\",tt_from:\"ttFrom\",tt_to:\"ttTo\"}," +
                              "blob:{type:\"bytes\"}," +
                              "bool:{type:\"boolean\"}," +
                              "date:{type:\"date\"}," +
                              "date_range:{type:\"date_range\",from:\"from\",to:\"to\"}," +
                              "double:{type:\"double\"}," +
                              "float:{type:\"float\"}," +
                              "geo:{type:\"geo_point\",latitude:\"lat\",longitude:\"lon\"}," +
                              "inet:{type:\"inet\"}," +
                              "int:{type:\"integer\",boost:0.3}," +
                              "long:{type:\"long\"}," +
                              "string:{type:\"string\"}," +
                              "text:{type:\"text\"}," +
                              "uuid:{type:\"uuid\"}}}";
        assertEquals("Failed schema JSON serialization", expectedJson, json);
    }

    @Test
    public void testFromJsonRegular() throws IOException {
        String json = "{analyzers:{" +
                      "custom:{type:\"classpath\",class:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}," +
                      "snowball:{type:\"snowball\",language:\"English\",stopwords:\"the,at\"}}," +
                      "default_analyzer:\"custom\"," +
                      "fields:" +
                      "{" +
                      "blob:{type:\"bytes\"}," +
                      "bool:{type:\"boolean\"}," +
                      "date:{type:\"date\"}," +
                      "inet:{type:\"inet\"}," +
                      "string:{type:\"string\"}," +
                      "text:{type:\"text\",analyzer:\"snowball\"}," +
                      "uuid:{type:\"uuid\"}" +
                      "}}";
        Schema schema = SchemaBuilder.fromJson(json).build();
        assertEquals("Failed schema JSON parsing", EnglishAnalyzer.class, schema.defaultAnalyzer.getClass());
        assertEquals("Failed schema JSON parsing", BlobMapper.class, schema.mapper("blob").getClass());
        assertEquals("Failed schema JSON parsing", BooleanMapper.class, schema.mapper("bool").getClass());
        assertEquals("Failed schema JSON parsing", DateMapper.class, schema.mapper("date").getClass());
        assertEquals("Failed schema JSON parsing", InetMapper.class, schema.mapper("inet").getClass());
        assertEquals("Failed schema JSON parsing", StringMapper.class, schema.mapper("string").getClass());
        assertEquals("Failed schema JSON parsing", TextMapper.class, schema.mapper("text").getClass());
        assertEquals("Failed schema JSON parsing", SnowballAnalyzer.class, schema.analyzer("text").getClass());
        assertEquals("Failed schema JSON parsing", SnowballAnalyzer.class, schema.analyzer("text.name").getClass());
        assertEquals("Failed schema JSON parsing", UUIDMapper.class, schema.mapper("uuid").getClass());
    }

    @Test
    public void testFromJsonNumeric() throws IOException {
        String json = "{analyzers:{" +
                      "custom:{type:\"classpath\",class:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}," +
                      "snowball:{type:\"snowball\",language:\"English\",stopwords:\"the,at\"}}," +
                      "default_analyzer:\"custom\"," +
                      "fields:" +
                      "{big_int:{type:\"bigint\",digits:10}," +
                      "big_dec:{type:\"bigdec\"}," +
                      "double:{type:\"double\"}," +
                      "float:{type:\"float\"}," +
                      "int:{type:\"integer\"}," +
                      "long:{type:\"long\"}}}";
        Schema schema = SchemaBuilder.fromJson(json).build();
        assertEquals("Failed schema JSON parsing", EnglishAnalyzer.class, schema.defaultAnalyzer.getClass());
        assertEquals("Failed schema JSON parsing", BigIntegerMapper.class, schema.mapper("big_int").getClass());
        assertEquals("Failed schema JSON parsing", BigDecimalMapper.class, schema.mapper("big_dec").getClass());
        assertEquals("Failed schema JSON parsing", DoubleMapper.class, schema.mapper("double").getClass());
        assertEquals("Failed schema JSON parsing", FloatMapper.class, schema.mapper("float").getClass());
        assertEquals("Failed schema JSON parsing", IntegerMapper.class, schema.mapper("int").getClass());
        assertEquals("Failed schema JSON parsing", LongMapper.class, schema.mapper("long").getClass());
    }

    @Test
    public void testFromJsonComplex() throws IOException {
        String json = "{analyzers:{" +
                      "custom:{type:\"classpath\",class:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}," +
                      "snowball:{type:\"snowball\",language:\"English\",stopwords:\"the,at\"}}," +
                      "default_analyzer:\"custom\"," +
                      "fields:" +
                      "{" +
                      "bitemporal:{type:\"bitemporal\",vt_from:\"vtFrom\",vt_to:\"vtTo\",tt_from:\"ttFrom\",tt_to:\"ttTo\"}," +
                      "date_range:{type:\"date_range\",from:\"from\",to:\"to\"}," +
                      "geo:{type:\"geo_point\",latitude:\"lat\",longitude:\"lon\"}" +
                      "}}";
        Schema schema = SchemaBuilder.fromJson(json).build();
        assertEquals("Failed schema JSON parsing", EnglishAnalyzer.class, schema.defaultAnalyzer.getClass());
        assertEquals("Failed schema JSON parsing", BitemporalMapper.class, schema.mapper("bitemporal").getClass());
        assertEquals("Failed schema JSON parsing", DateRangeMapper.class, schema.mapper("date_range").getClass());
        assertEquals("Failed schema JSON parsing", GeoPointMapper.class, schema.mapper("geo").getClass());
    }

    @Test
    public void testFromJSONWithNullAnalyzers() throws IOException {

        String json = "{" +
                      "  default_analyzer : \"org.apache.lucene.analysis.en.EnglishAnalyzer\"," +
                      "  fields : {" +
                      "    id : {type : \"integer\"}," +
                      "    spanish_text : {" +
                      "      type:\"text\", " +
                      "      analyzer:\"org.apache.lucene.analysis.es.SpanishAnalyzer\"}," +
                      "    snowball_text : {" +
                      "      type:\"text\", " +
                      "      analyzer:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}," +
                      "    default_text : { type:\"text\"}" +
                      "  }" +
                      " }'";

        Schema schema = SchemaBuilder.fromJson(json).build();

        Analyzer defaultAnalyzer = schema.defaultAnalyzer;
        assertTrue("Expected english analyzer", defaultAnalyzer instanceof EnglishAnalyzer);

        Mapper idMapper = schema.mapper("id");
        assertTrue("Expected IntegerMapper", idMapper instanceof IntegerMapper);

        Mapper spanishMapper = schema.mapper("spanish_text");
        assertTrue("Expected TextMapper", spanishMapper instanceof TextMapper);
        assertEquals("Expected spanish analyzer", SpanishAnalyzer.class.getName(), spanishMapper.analyzer);

        Mapper snowballMapper = schema.mapper("snowball_text");
        assertTrue("Expected TextMapper", snowballMapper instanceof TextMapper);
        assertEquals("Expected english analyzer", EnglishAnalyzer.class.getName(), snowballMapper.analyzer);

        Mapper defaultMapper = schema.mapper("default_text");
        assertTrue("Expected TextMapper", defaultMapper instanceof TextMapper);
        assertEquals("Expected english analyzer", EnglishAnalyzer.class.getName(), snowballMapper.analyzer);

        schema.close();
    }

    @Test
    public void testFromJSONWithEmptyAnalyzers() throws IOException {

        String json = "{" +
                      "  analyzers:{}, " +
                      "  default_analyzer : \"org.apache.lucene.analysis.en.EnglishAnalyzer\"," +
                      "  fields : {" +
                      "    id : {type : \"integer\"}," +
                      "    spanish_text : {type:\"text\", " +
                      "                    analyzer:\"org.apache.lucene.analysis.es.SpanishAnalyzer\"}," +
                      "    snowball_text : {type:\"text\", " +
                      "                     analyzer:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}" +
                      "  }" +
                      " }'";
        Schema schema = SchemaBuilder.fromJson(json).build();

        Analyzer defaultAnalyzer = schema.defaultAnalyzer;
        assertTrue("Expected EnglishAnalyzer", defaultAnalyzer instanceof EnglishAnalyzer);

        Mapper idMapper = schema.mapper("id");
        assertEquals("Expected IntegerMapper", IntegerMapper.class, idMapper.getClass());

        Mapper spanishMapper = schema.mapper("spanish_text");
        assertTrue(spanishMapper instanceof TextMapper);
        assertEquals("Expected SpanishAnalyzer", SpanishAnalyzer.class.getName(), spanishMapper.analyzer);

        Mapper snowballMapper = schema.mapper("snowball_text");
        assertTrue(snowballMapper instanceof TextMapper);
        assertEquals("Expected EnglishAnalyzer", EnglishAnalyzer.class.getName(), snowballMapper.analyzer);

        schema.close();
    }

    @Test
    public void testParseJSONWithNullDefaultAnalyzer() throws IOException {

        String json = "{" +
                      "  analyzers:{" +
                      "    spanish_analyzer : {" +
                      "      type:\"classpath\", " +
                      "      class:\"org.apache.lucene.analysis.es.SpanishAnalyzer\"}," +
                      "    snowball_analyzer : {" +
                      "      type:\"snowball\", " +
                      "      language:\"Spanish\", " +
                      "      stopwords : \"el,la,lo,lo,as,las,a,ante,con,contra\"}" +
                      "  }," +
                      "  fields : { id : {type : \"integer\"}, text : {type : \"text\"} }" +
                      " }'";
        Schema schema = SchemaBuilder.fromJson(json).build();

        Analyzer defaultAnalyzer = schema.defaultAnalyzer;
        assertEquals("Expected default analyzer",
                     StandardAnalyzers.DEFAULT.get().getClass(),
                     defaultAnalyzer.getClass());

        Analyzer textAnalyzer = schema.analyzer("text");
        assertEquals("Expected default analyzer", StandardAnalyzers.DEFAULT.get().getClass(), textAnalyzer.getClass());
        textAnalyzer = schema.analyzer("text.name");
        assertEquals("Expected default analyzer", StandardAnalyzers.DEFAULT.get().getClass(), textAnalyzer.getClass());

        schema.close();
    }

    @Test(expected = IndexException.class)
    public void testParseJSONWithFailingDefaultAnalyzer() throws IOException {
        String json = "{default_analyzer : \"xyz\", fields : { id : {type : \"integer\"} } }'";
        SchemaBuilder.fromJson(json).build();
    }
}
