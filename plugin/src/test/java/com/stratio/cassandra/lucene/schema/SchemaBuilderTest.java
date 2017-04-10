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

package com.stratio.cassandra.lucene.schema;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.analysis.ComplexAnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.analysis.SnowballAnalyzerBuilder.SnowballAnalyzer;
import com.stratio.cassandra.lucene.schema.analysis.StandardAnalyzers;
import com.stratio.cassandra.lucene.schema.mapping.BigDecimalMapper;
import com.stratio.cassandra.lucene.schema.mapping.BigIntegerMapper;
import com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper;
import com.stratio.cassandra.lucene.schema.mapping.BlobMapper;
import com.stratio.cassandra.lucene.schema.mapping.BooleanMapper;
import com.stratio.cassandra.lucene.schema.mapping.DateMapper;
import com.stratio.cassandra.lucene.schema.mapping.DateRangeMapper;
import com.stratio.cassandra.lucene.schema.mapping.DoubleMapper;
import com.stratio.cassandra.lucene.schema.mapping.FloatMapper;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.InetMapper;
import com.stratio.cassandra.lucene.schema.mapping.IntegerMapper;
import com.stratio.cassandra.lucene.schema.mapping.LongMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.schema.mapping.StringMapper;
import com.stratio.cassandra.lucene.schema.mapping.TextMapper;
import com.stratio.cassandra.lucene.schema.mapping.UUIDMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.bigDecimalMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.bigIntegerMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.bitemporalMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.blobMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.booleanMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.classpathAnalyzer;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.dateMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.dateRangeMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.doubleMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.floatMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.geoPointMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.inetMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.integerMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.longMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.snowballAnalyzer;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.stringMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.textMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.uuidMapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
        assertEquals("Failed schema building", EnglishAnalyzer.class, schema.getDefaultAnalyzer().getClass());
        assertEquals("Failed schema building", BlobMapper.class, schema.getMapper("blob").getClass());
        assertEquals("Failed schema building", BooleanMapper.class, schema.getMapper("bool").getClass());
        assertEquals("Failed schema building", DateMapper.class, schema.getMapper("date").getClass());
        assertEquals("Failed schema building", InetMapper.class, schema.getMapper("inet").getClass());
        assertEquals("Failed schema building", StringMapper.class, schema.getMapper("string").getClass());
        assertEquals("Failed schema building", TextMapper.class, schema.getMapper("text").getClass());
        assertEquals("Failed schema building", SnowballAnalyzer.class, schema.getAnalyzer("text").getClass());
        assertEquals("Failed schema building", UUIDMapper.class, schema.getMapper("uuid").getClass());
    }

    @Test
    public void testBuildNumeric() throws Exception {
        Schema schema = schema().defaultAnalyzer("custom")
                                .analyzer("custom", classpathAnalyzer("org.apache.lucene.analysis.en.EnglishAnalyzer"))
                                .analyzer("snowball", snowballAnalyzer("English", "the,at"))
                                .mapper("big_int", bigIntegerMapper().digits(10))
                                .mapper("big_dec", bigDecimalMapper().indexed(false).sorted(true))
                                .mapper("double", doubleMapper())
                                .mapper("float", floatMapper())
                                .mapper("int", integerMapper().boost(0.3f))
                                .mapper("long", longMapper())
                                .build();
        assertEquals("Failed schema building", EnglishAnalyzer.class, schema.getDefaultAnalyzer().getClass());
        assertEquals("Failed schema building", BigIntegerMapper.class, schema.getMapper("big_int").getClass());
        assertEquals("Failed schema building", BigDecimalMapper.class, schema.getMapper("big_dec").getClass());
        assertEquals("Failed schema building", DoubleMapper.class, schema.getMapper("double").getClass());
        assertEquals("Failed schema building", FloatMapper.class, schema.getMapper("float").getClass());
        assertEquals("Failed schema building", IntegerMapper.class, schema.getMapper("int").getClass());
        assertEquals("Failed schema building", LongMapper.class, schema.getMapper("long").getClass());
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
        assertEquals("Failed schema building", EnglishAnalyzer.class, schema.getDefaultAnalyzer().getClass());
        assertEquals("Failed schema building", BitemporalMapper.class, schema.getMapper("bitemporal").getClass());
        assertEquals("Failed schema building", DateRangeMapper.class, schema.getMapper("date_range").getClass());
        assertEquals("Failed schema building", GeoPointMapper.class, schema.getMapper("geo").getClass());
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
        assertEquals("Failed schema JSON parsing", EnglishAnalyzer.class, schema.getDefaultAnalyzer().getClass());
        assertEquals("Failed schema JSON parsing", BlobMapper.class, schema.getMapper("blob").getClass());
        assertEquals("Failed schema JSON parsing", BooleanMapper.class, schema.getMapper("bool").getClass());
        assertEquals("Failed schema JSON parsing", DateMapper.class, schema.getMapper("date").getClass());
        assertEquals("Failed schema JSON parsing", InetMapper.class, schema.getMapper("inet").getClass());
        assertEquals("Failed schema JSON parsing", StringMapper.class, schema.getMapper("string").getClass());
        assertEquals("Failed schema JSON parsing", TextMapper.class, schema.getMapper("text").getClass());
        assertEquals("Failed schema JSON parsing", SnowballAnalyzer.class, schema.getAnalyzer("text").getClass());
        assertEquals("Failed schema JSON parsing", SnowballAnalyzer.class, schema.getAnalyzer("text.name").getClass());
        assertEquals("Failed schema JSON parsing", UUIDMapper.class, schema.getMapper("uuid").getClass());
    }

    @Test
    public void testFromJsonNumeric() throws IOException {
        String json = "{analyzers:{" +
                      "custom:{type:\"classpath\",class:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}," +
                      "snowball:{type:\"snowball\",language:\"English\",stopwords:\"the,at\"}}," +
                      "default_analyzer:\"custom\"," +
                      "fields:" +
                      "{big_int:{type:\"bigint\",digits:10}," +
                      "big_dec:{type:\"bigdec\",indexed:false,sorted:true}," +
                      "double:{type:\"double\"}," +
                      "float:{type:\"float\"}," +
                      "int:{type:\"integer\"}," +
                      "long:{type:\"long\"}}}";
        Schema schema = SchemaBuilder.fromJson(json).build();
        assertEquals("Failed schema JSON parsing", EnglishAnalyzer.class, schema.getDefaultAnalyzer().getClass());
        assertEquals("Failed schema JSON parsing", BigIntegerMapper.class, schema.getMapper("big_int").getClass());
        assertEquals("Failed schema JSON parsing", BigDecimalMapper.class, schema.getMapper("big_dec").getClass());
        assertEquals("Failed schema JSON parsing", DoubleMapper.class, schema.getMapper("double").getClass());
        assertEquals("Failed schema JSON parsing", FloatMapper.class, schema.getMapper("float").getClass());
        assertEquals("Failed schema JSON parsing", IntegerMapper.class, schema.getMapper("int").getClass());
        assertEquals("Failed schema JSON parsing", LongMapper.class, schema.getMapper("long").getClass());
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
        assertEquals("Failed schema JSON parsing", EnglishAnalyzer.class, schema.getDefaultAnalyzer().getClass());
        assertEquals("Failed schema JSON parsing", BitemporalMapper.class, schema.getMapper("bitemporal").getClass());
        assertEquals("Failed schema JSON parsing", DateRangeMapper.class, schema.getMapper("date_range").getClass());
        assertEquals("Failed schema JSON parsing", GeoPointMapper.class, schema.getMapper("geo").getClass());
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
    public void testFromJSONWithComplexAnalyzer() throws IOException {
        final String complexJson = "{" +
                "type:\"complex\"," +
                "tokenizer:{\"class\":\"ngram\", \"parameters\":[\"1\",\"2\"]}," +
                "token_streams:[" +
                "  {" +
                "\"class\":\"stop\"," +
                "\"parameters\":[null, \"a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on," +
                "or,such,that,the,their,then,there,these,they,this,to,was,will,with\"]" +
                "  }," +
                "  {" +
                "\"class\":\"org.apache.lucene.analysis.core.LowerCaseFilter\"," +
                "\"parameters\":[null]" +
                "  }," +
                "  {" +
                "\"class\":\"org.apache.lucene.analysis.standard.StandardFilter\"," +
                "\"parameters\":[null]" +
                "  }" +
                "]}";
        final String json = "{analyzers:{\"customandcomplex\":" + complexJson + "}, default_analyzer : \"customandcomplex\" }'";
        final Schema schema = SchemaBuilder.fromJson(json).build();

        final Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        assertNotNull(defaultAnalyzer);
        assertTrue(ComplexAnalyzerBuilder.ComplexAnalyzer.class.isInstance(defaultAnalyzer));

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
                     StandardAnalyzers.DEFAULT.get().getClass(),
                     defaultAnalyzer.getClass());

        Analyzer textAnalyzer = schema.getAnalyzer("text");
        assertEquals("Expected default analyzer", StandardAnalyzers.DEFAULT.get().getClass(), textAnalyzer.getClass());
        textAnalyzer = schema.getAnalyzer("text.name");
        assertEquals("Expected default analyzer", StandardAnalyzers.DEFAULT.get().getClass(), textAnalyzer.getClass());

        schema.close();
    }

    @Test(expected = IndexException.class)
    public void testParseJSONWithFailingDefaultAnalyzer() throws IOException {
        String json = "{default_analyzer : \"xyz\", fields : { id : {type : \"integer\"} } }'";
        SchemaBuilder.fromJson(json).build();
    }
}
