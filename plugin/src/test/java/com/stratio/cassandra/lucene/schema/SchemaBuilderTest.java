/*
 * Copyright 2015, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.cassandra.lucene.schema;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import com.stratio.cassandra.lucene.schema.analysis.SnowballAnalyzerBuilder.SnowballAnalyzer;
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
                                .mapper("big_int", bigIntegerMapper().digits(10))
                                .mapper("big_dec", bigDecimalMapper().indexed(false).sorted(true))
                                .mapper("bitemporal", bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to"))
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
                                .mapper("text", textMapper().analyzer("snowball"))
                                .mapper("uuid", uuidMapper())
                                .build();
        assertEquals("Failed schema building", EnglishAnalyzer.class, schema.getDefaultAnalyzer().getClass());
        assertEquals("Failed schema building", BigIntegerMapper.class, schema.getMapper("big_int").getClass());
        assertEquals("Failed schema building", BigDecimalMapper.class, schema.getMapper("big_dec").getClass());
        assertEquals("Failed schema building", BitemporalMapper.class, schema.getMapper("bitemporal").getClass());
        assertEquals("Failed schema building", BlobMapper.class, schema.getMapper("blob").getClass());
        assertEquals("Failed schema building", BooleanMapper.class, schema.getMapper("bool").getClass());
        assertEquals("Failed schema building", DateMapper.class, schema.getMapper("date").getClass());
        assertEquals("Failed schema building", DateRangeMapper.class, schema.getMapper("date_range").getClass());
        assertEquals("Failed schema building", DoubleMapper.class, schema.getMapper("double").getClass());
        assertEquals("Failed schema building", FloatMapper.class, schema.getMapper("float").getClass());
        assertEquals("Failed schema building", GeoPointMapper.class, schema.getMapper("geo").getClass());
        assertEquals("Failed schema building", InetMapper.class, schema.getMapper("inet").getClass());
        assertEquals("Failed schema building", IntegerMapper.class, schema.getMapper("int").getClass());
        assertEquals("Failed schema building", LongMapper.class, schema.getMapper("long").getClass());
        assertEquals("Failed schema building", StringMapper.class, schema.getMapper("string").getClass());
        assertEquals("Failed schema building", TextMapper.class, schema.getMapper("text").getClass());
        assertEquals("Failed schema building", SnowballAnalyzer.class, schema.getAnalyzer("text").getClass());
        assertEquals("Failed schema building", UUIDMapper.class, schema.getMapper("uuid").getClass());
    }

    @Test
    public void testToJson() throws IOException {
        String json = schema().defaultAnalyzer("custom")
                              .analyzer("custom", classpathAnalyzer("org.apache.lucene.analysis.en.EnglishAnalyzer"))
                              .analyzer("snowball", snowballAnalyzer("English", "the,at"))
                              .mapper("big_int", bigIntegerMapper().digits(10))
                              .mapper("big_dec", bigDecimalMapper().indexed(false).sorted(true))
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
                              "big_dec:{type:\"bigdec\",indexed:false,sorted:true}," +
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
    public void testFromJson() throws IOException {
        String json = "{analyzers:{" +
                      "custom:{type:\"classpath\",class:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}," +
                      "snowball:{type:\"snowball\",language:\"English\",stopwords:\"the,at\"}}," +
                      "default_analyzer:\"custom\"," +
                      "fields:" +
                      "{big_int:{type:\"bigint\",digits:10}," +
                      "big_dec:{type:\"bigdec\",indexed:false,sorted:true}," +
                      "bitemporal:{type:\"bitemporal\",vt_from:\"vtFrom\",vt_to:\"vtTo\",tt_from:\"ttFrom\",tt_to:\"ttTo\"}," +
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
                      "text:{type:\"text\",analyzer:\"snowball\"}," +
                      "uuid:{type:\"uuid\"}}}";
        Schema schema = SchemaBuilder.fromJson(json).build();
        assertEquals("Failed schema JSON parsing", EnglishAnalyzer.class, schema.getDefaultAnalyzer().getClass());
        assertEquals("Failed schema JSON parsing", BigIntegerMapper.class, schema.getMapper("big_int").getClass());
        assertEquals("Failed schema JSON parsing", BigDecimalMapper.class, schema.getMapper("big_dec").getClass());
        assertEquals("Failed schema JSON parsing", BitemporalMapper.class, schema.getMapper("bitemporal").getClass());
        assertEquals("Failed schema JSON parsing", BlobMapper.class, schema.getMapper("blob").getClass());
        assertEquals("Failed schema JSON parsing", BooleanMapper.class, schema.getMapper("bool").getClass());
        assertEquals("Failed schema JSON parsing", DateMapper.class, schema.getMapper("date").getClass());
        assertEquals("Failed schema JSON parsing", DateRangeMapper.class, schema.getMapper("date_range").getClass());
        assertEquals("Failed schema JSON parsing", DoubleMapper.class, schema.getMapper("double").getClass());
        assertEquals("Failed schema JSON parsing", FloatMapper.class, schema.getMapper("float").getClass());
        assertEquals("Failed schema JSON parsing", GeoPointMapper.class, schema.getMapper("geo").getClass());
        assertEquals("Failed schema JSON parsing", InetMapper.class, schema.getMapper("inet").getClass());
        assertEquals("Failed schema JSON parsing", IntegerMapper.class, schema.getMapper("int").getClass());
        assertEquals("Failed schema JSON parsing", LongMapper.class, schema.getMapper("long").getClass());
        assertEquals("Failed schema JSON parsing", StringMapper.class, schema.getMapper("string").getClass());
        assertEquals("Failed schema JSON parsing", TextMapper.class, schema.getMapper("text").getClass());
        assertEquals("Failed schema JSON parsing", SnowballAnalyzer.class, schema.getAnalyzer("text").getClass());
        assertEquals("Failed schema JSON parsing", SnowballAnalyzer.class, schema.getAnalyzer("text.name").getClass());
        assertEquals("Failed schema JSON parsing", UUIDMapper.class, schema.getMapper("uuid").getClass());
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

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        assertTrue("Expected english analyzer", defaultAnalyzer instanceof EnglishAnalyzer);

        Mapper idMapper = schema.getMapper("id");
        assertTrue("Expected IntegerMapper", idMapper instanceof IntegerMapper);

        Mapper spanishMapper = schema.getMapper("spanish_text");
        assertTrue("Expected TextMapper", spanishMapper instanceof TextMapper);
        assertEquals("Expected spanish analyzer", SpanishAnalyzer.class.getName(), spanishMapper.analyzer);

        Mapper snowballMapper = schema.getMapper("snowball_text");
        assertTrue("Expected TextMapper", snowballMapper instanceof TextMapper);
        assertEquals("Expected english analyzer", EnglishAnalyzer.class.getName(), snowballMapper.analyzer);

        Mapper defaultMapper = schema.getMapper("default_text");
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

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        assertTrue("Expected EnglishAnalyzer", defaultAnalyzer instanceof EnglishAnalyzer);

        Mapper idMapper = schema.getMapper("id");
        assertEquals("Expected IntegerMapper", IntegerMapper.class, idMapper.getClass());

        Mapper spanishMapper = schema.getMapper("spanish_text");
        assertTrue(spanishMapper instanceof TextMapper);
        assertEquals("Expected SpanishAnalyzer", SpanishAnalyzer.class.getName(), spanishMapper.analyzer);

        Mapper snowballMapper = schema.getMapper("snowball_text");
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

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        assertEquals("Expected default analyzer",
                     PreBuiltAnalyzers.DEFAULT.get().getClass(),
                     defaultAnalyzer.getClass());

        Analyzer textAnalyzer = schema.getAnalyzer("text");
        assertEquals("Expected default analyzer", PreBuiltAnalyzers.DEFAULT.get().getClass(), textAnalyzer.getClass());
        textAnalyzer = schema.getAnalyzer("text.name");
        assertEquals("Expected default analyzer", PreBuiltAnalyzers.DEFAULT.get().getClass(), textAnalyzer.getClass());

        schema.close();
    }

    @Test(expected = IndexException.class)
    public void testParseJSONWithFailingDefaultAnalyzer() throws IOException {
        String json = "{default_analyzer : \"xyz\", fields : { id : {type : \"integer\"} } }'";
        SchemaBuilder.fromJson(json).build();
    }
}
