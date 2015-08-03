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
                                .mapper("date_range", dateRangeMapper("start", "stop"))
                                .mapper("double", doubleMapper())
                                .mapper("float", floatMapper())
                                .mapper("geo", geoPointMapper("lat", "lon"))
                                .mapper("inet", inetMapper())
                                .mapper("int", integerMapper().boost(0.3f))
                                .mapper("long", longMapper())
                                .mapper("string", stringMapper())
                                .mapper("text", textMapper())
                                .mapper("uuid", UUIDMapper())
                                .build();
        assertEquals(EnglishAnalyzer.class, schema.getDefaultAnalyzer().getClass());
        assertEquals(EnglishAnalyzer.class, schema.getAnalyzer("custom").getClass());
        assertEquals(SnowballAnalyzer.class, schema.getAnalyzer("snowball").getClass());
        assertEquals(BigIntegerMapper.class, schema.getMapper("big_int").getClass());
        assertEquals(BigDecimalMapper.class, schema.getMapper("big_dec").getClass());
        assertEquals(BitemporalMapper.class, schema.getMapper("bitemporal").getClass());
        assertEquals(BlobMapper.class, schema.getMapper("blob").getClass());
        assertEquals(BooleanMapper.class, schema.getMapper("bool").getClass());
        assertEquals(DateMapper.class, schema.getMapper("date").getClass());
        assertEquals(DateRangeMapper.class, schema.getMapper("date_range").getClass());
        assertEquals(DoubleMapper.class, schema.getMapper("double").getClass());
        assertEquals(FloatMapper.class, schema.getMapper("float").getClass());
        assertEquals(GeoPointMapper.class, schema.getMapper("geo").getClass());
        assertEquals(InetMapper.class, schema.getMapper("inet").getClass());
        assertEquals(IntegerMapper.class, schema.getMapper("int").getClass());
        assertEquals(LongMapper.class, schema.getMapper("long").getClass());
        assertEquals(StringMapper.class, schema.getMapper("string").getClass());
        assertEquals(TextMapper.class, schema.getMapper("text").getClass());
        assertEquals(UUIDMapper.class, schema.getMapper("uuid").getClass());
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
                              .mapper("date_range", dateRangeMapper("start", "stop"))
                              .mapper("double", doubleMapper())
                              .mapper("float", floatMapper())
                              .mapper("geo", geoPointMapper("lat", "lon"))
                              .mapper("inet", inetMapper())
                              .mapper("int", integerMapper().boost(0.3f))
                              .mapper("long", longMapper())
                              .mapper("string", stringMapper())
                              .mapper("text", textMapper())
                              .mapper("uuid", UUIDMapper())
                              .toJson();
        assertEquals("{default_analyzer:\"custom\"," +
                     "analyzers:{" +
                     "custom:{type:\"classpath\",class:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}," +
                     "snowball:{type:\"snowball\",language:\"English\",stopwords:\"the,at\"}}," +
                     "fields:" +
                     "{big_int:{type:\"bigint\",digits:10}," +
                     "big_dec:{type:\"bigdec\",indexed:false,sorted:true}," +
                     "bitemporal:{type:\"bitemporal\",vt_from:\"vtFrom\",vt_to:\"vtTo\",tt_from:\"ttFrom\",tt_to:\"ttTo\"}," +
                     "blob:{type:\"bytes\"}," +
                     "bool:{type:\"boolean\"}," +
                     "date:{type:\"date\"}," +
                     "date_range:{type:\"date_range\",start:\"start\",stop:\"stop\"}," +
                     "double:{type:\"double\"}," +
                     "float:{type:\"float\"}," +
                     "geo:{type:\"geo_point\",latitude:\"lat\",longitude:\"lon\"}," +
                     "inet:{type:\"inet\"}," +
                     "int:{type:\"integer\",boost:0.3}," +
                     "long:{type:\"long\"}," +
                     "string:{type:\"string\"}," +
                     "text:{type:\"text\"}," +
                     "uuid:{type:\"uuid\"}}}", json);
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
                      "date_range:{type:\"date_range\",start:\"start\",stop:\"stop\"}," +
                      "double:{type:\"double\"}," +
                      "float:{type:\"float\"}," +
                      "geo:{type:\"geo_point\",latitude:\"lat\",longitude:\"lon\"}," +
                      "inet:{type:\"inet\"}," +
                      "int:{type:\"integer\",boost:0.3}," +
                      "long:{type:\"long\"}," +
                      "string:{type:\"string\"}," +
                      "text:{type:\"text\"}," +
                      "uuid:{type:\"uuid\"}}}";
        Schema schema = SchemaBuilder.fromJson(json).build();
        assertEquals(EnglishAnalyzer.class, schema.getDefaultAnalyzer().getClass());
        assertEquals(EnglishAnalyzer.class, schema.getAnalyzer("custom").getClass());
        assertEquals(SnowballAnalyzer.class, schema.getAnalyzer("snowball").getClass());
        assertEquals(BigIntegerMapper.class, schema.getMapper("big_int").getClass());
        assertEquals(BigDecimalMapper.class, schema.getMapper("big_dec").getClass());
        assertEquals(BitemporalMapper.class, schema.getMapper("bitemporal").getClass());
        assertEquals(BlobMapper.class, schema.getMapper("blob").getClass());
        assertEquals(BooleanMapper.class, schema.getMapper("bool").getClass());
        assertEquals(DateMapper.class, schema.getMapper("date").getClass());
        assertEquals(DateRangeMapper.class, schema.getMapper("date_range").getClass());
        assertEquals(DoubleMapper.class, schema.getMapper("double").getClass());
        assertEquals(FloatMapper.class, schema.getMapper("float").getClass());
        assertEquals(GeoPointMapper.class, schema.getMapper("geo").getClass());
        assertEquals(InetMapper.class, schema.getMapper("inet").getClass());
        assertEquals(IntegerMapper.class, schema.getMapper("int").getClass());
        assertEquals(LongMapper.class, schema.getMapper("long").getClass());
        assertEquals(StringMapper.class, schema.getMapper("string").getClass());
        assertEquals(TextMapper.class, schema.getMapper("text").getClass());
        assertEquals(UUIDMapper.class, schema.getMapper("uuid").getClass());
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
                      "      analyzer:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}" +
                      "  }" +
                      " }'";

        Schema schema = SchemaBuilder.fromJson(json).build();

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        assertTrue(defaultAnalyzer instanceof EnglishAnalyzer);

        Mapper idMapper = schema.getMapper("id");
        assertTrue(idMapper instanceof IntegerMapper);

        Mapper spanishMapper = schema.getMapper("spanish_text");
        assertTrue(spanishMapper instanceof TextMapper);
        assertEquals(SpanishAnalyzer.class.getName(), spanishMapper.getAnalyzer());

        Mapper snowballMapper = schema.getMapper("snowball_text");
        assertTrue(snowballMapper instanceof TextMapper);
        assertEquals(EnglishAnalyzer.class.getName(), snowballMapper.getAnalyzer());

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
        assertTrue(defaultAnalyzer instanceof EnglishAnalyzer);

        Mapper idMapper = schema.getMapper("id");
        assertEquals(IntegerMapper.class, idMapper.getClass());

        Mapper spanishMapper = schema.getMapper("spanish_text");
        assertTrue(spanishMapper instanceof TextMapper);
        assertEquals(SpanishAnalyzer.class.getName(), spanishMapper.getAnalyzer());

        Mapper snowballMapper = schema.getMapper("snowball_text");
        assertTrue(snowballMapper instanceof TextMapper);
        assertEquals(EnglishAnalyzer.class.getName(), snowballMapper.getAnalyzer());

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
                      "  fields : { id : {type : \"integer\"} }" +
                      " }'";
        Schema schema = SchemaBuilder.fromJson(json).build();

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        assertEquals(PreBuiltAnalyzers.DEFAULT.get().getClass(), defaultAnalyzer.getClass());

        Analyzer spanishAnalyzer = schema.getAnalyzer("spanish_analyzer");
        assertTrue(spanishAnalyzer instanceof SpanishAnalyzer);

        schema.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseJSONWithFailingDefaultAnalyzer() throws IOException {
        String json = "{default_analyzer : \"xyz\", fields : { id : {type : \"integer\"} } }'";
        SchemaBuilder.fromJson(json).build();
    }
}
