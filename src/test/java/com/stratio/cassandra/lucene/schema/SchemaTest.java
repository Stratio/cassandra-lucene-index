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
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInteger;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperText;
import com.stratio.cassandra.lucene.schema.mapping.builder.ColumnMapperBuilder;
import com.stratio.cassandra.lucene.schema.mapping.builder.ColumnMapperIntegerBuilder;
import com.stratio.cassandra.lucene.schema.mapping.builder.ColumnMapperStringBuilder;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.document.Document;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SchemaTest {

    @Test
    public void testGetDefaultAnalyzer() {
        Map<String, ColumnMapperBuilder> columnMappers = new HashMap<>();
        Schema schema = new Schema(columnMappers, null, "English");
        Analyzer analyzer = schema.getDefaultAnalyzer();
        assertEquals(EnglishAnalyzer.class, analyzer.getClass());
        schema.close();
    }

    @Test
    public void testGetDefaultAnalyzerNotSpecified() {
        Map<String, ColumnMapperBuilder> columnMappers = new HashMap<>();
        Schema schema = new Schema(columnMappers, null, null);
        Analyzer analyzer = schema.getDefaultAnalyzer();
        assertEquals(PreBuiltAnalyzers.DEFAULT.get(), analyzer);
        schema.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerNotExistent() {
        Map<String, ColumnMapperBuilder> columnMappers = new HashMap<>();
        Schema schema = new Schema(columnMappers, null, "English");
        schema.getAnalyzer("custom");
        schema.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerNull() {
        Map<String, ColumnMapperBuilder> columnMappers = new HashMap<>();
        Schema schema = new Schema(columnMappers, null, "English");
        schema.getAnalyzer(null);
        schema.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerEmpty() {
        Map<String, ColumnMapperBuilder> columnMappers = new HashMap<>();
        Schema schema = new Schema(columnMappers, null, "English");
        schema.getAnalyzer(" \t");
        schema.close();
    }

    @Test
    public void testParseJSON() throws IOException {

        String json = "{" +
                      "  analyzers:{" +
                      "    spanish_analyzer : {type:\"classpath\", " +
                      "                        class:\"org.apache.lucene.analysis.es.SpanishAnalyzer\"}," +
                      "    snowball_analyzer : {type:\"snowball\", " +
                      "                         language:\"Spanish\", " +
                      "                         stopwords : \"el,la,lo,loas,las,a,ante,bajo,cabe,con,contra\"}" +
                      "  }," +
                      "  default_analyzer : \"spanish_analyzer\"," +
                      "  fields : {" +
                      "    id : {type : \"integer\"}," +
                      "    spanish_text : {type:\"text\", analyzer:\"spanish_analyzer\"}," +
                      "    snowball_text : {type:\"text\", analyzer:\"snowball_analyzer\"}" +
                      "  }" +
                      " }'";

        Schema schema = JsonSerializer.fromString(json, Schema.class);

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        assertTrue(defaultAnalyzer instanceof SpanishAnalyzer);

        Analyzer spanishAnalyzer = schema.getAnalyzer("spanish_analyzer");
        assertTrue(spanishAnalyzer instanceof SpanishAnalyzer);

        ColumnMapper idMapper = schema.getMapper("id");
        assertTrue(idMapper instanceof ColumnMapperInteger);

        ColumnMapper spanishMapper = schema.getMapper("spanish_text");
        assertTrue(spanishMapper instanceof ColumnMapperText);
        assertEquals("spanish_analyzer", spanishMapper.getAnalyzer());

        ColumnMapper snowballMapper = schema.getMapper("snowball_text");
        assertTrue(snowballMapper instanceof ColumnMapperText);
        assertEquals("snowball_analyzer", snowballMapper.getAnalyzer());

        schema.close();
    }

    @Test
    public void testParseJSONWithNullAnalyzers() throws IOException {

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

        Schema schema = JsonSerializer.fromString(json, Schema.class);

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        assertTrue(defaultAnalyzer instanceof EnglishAnalyzer);

        ColumnMapper idMapper = schema.getMapper("id");
        assertTrue(idMapper instanceof ColumnMapperInteger);

        ColumnMapper spanishMapper = schema.getMapper("spanish_text");
        assertTrue(spanishMapper instanceof ColumnMapperText);
        assertEquals(SpanishAnalyzer.class.getName(), spanishMapper.getAnalyzer());

        ColumnMapper snowballMapper = schema.getMapper("snowball_text");
        assertTrue(snowballMapper instanceof ColumnMapperText);
        assertEquals(EnglishAnalyzer.class.getName(), snowballMapper.getAnalyzer());

        schema.close();
    }

    @Test
    public void testParseJSONWithEmptyAnalyzers() throws IOException {

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

        Schema schema = JsonSerializer.fromString(json, Schema.class);

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        assertTrue(defaultAnalyzer instanceof EnglishAnalyzer);

        ColumnMapper idMapper = schema.getMapper("id");
        assertEquals(ColumnMapperInteger.class, idMapper.getClass());

        ColumnMapper spanishMapper = schema.getMapper("spanish_text");
        assertTrue(spanishMapper instanceof ColumnMapperText);
        assertEquals(SpanishAnalyzer.class.getName(), spanishMapper.getAnalyzer());

        ColumnMapper snowballMapper = schema.getMapper("snowball_text");
        assertTrue(snowballMapper instanceof ColumnMapperText);
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

        Schema schema = JsonSerializer.fromString(json, Schema.class);

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        assertEquals(PreBuiltAnalyzers.DEFAULT.get(), defaultAnalyzer);

        Analyzer spanishAnalyzer = schema.getAnalyzer("spanish_analyzer");
        assertTrue(spanishAnalyzer instanceof SpanishAnalyzer);

        schema.close();
    }

    @Test(expected = JsonMappingException.class)
    public void testParseJSONWithFailingDefaultAnalyzer() throws IOException {
        String json = "{default_analyzer : \"xyz\", fields : { id : {type : \"integer\"} } }'";
        JsonSerializer.fromString(json, Schema.class);
    }

    @Test
    public void testAddColumns() {

        ColumnMapperBuilder columnMapper1 = new ColumnMapperStringBuilder();
        ColumnMapperBuilder columnMapper2 = new ColumnMapperIntegerBuilder();

        Map<String, ColumnMapperBuilder> columnMappers = new HashMap<>();
        columnMappers.put("field1", columnMapper1);
        columnMappers.put("field2", columnMapper2);

        Schema schema = new Schema(columnMappers, null, null);

        Columns columns = new Columns().add(Column.fromComposed("field1", "value", UTF8Type.instance, false))
                                       .add(Column.fromComposed("field2", 1L, LongType.instance, false));

        Document document = new Document();
        schema.addFields(document, columns);
        assertEquals(4, document.getFields().size());
        assertEquals(2, document.getFields("field1").length);
        assertEquals(2, document.getFields("field2").length);

        schema.close();
    }

    @Test
    public void testValidate() throws InvalidRequestException, ConfigurationException {

        List<ColumnDef> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field1"),
                                            UTF8Type.class.getCanonicalName()).setIndex_name("field1")
                                                                              .setIndex_type(IndexType.KEYS));

        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field2"),
                                            IntegerType.class.getCanonicalName()).setIndex_name("field2")
                                                                                 .setIndex_type(IndexType.KEYS));
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setColumn_metadata(columnDefinitions)
                                 .setKeyspace("Keyspace1")
                                 .setName("Standard1");
        CFMetaData metadata = CFMetaData.fromThrift(cfDef);

        ColumnMapperBuilder columnMapper1 = new ColumnMapperStringBuilder();
        ColumnMapperBuilder columnMapper2 = new ColumnMapperIntegerBuilder();

        Map<String, ColumnMapperBuilder> columnMappers = new HashMap<>();
        columnMappers.put("field1", columnMapper1);
        columnMappers.put("field2", columnMapper2);

        Schema schema = new Schema(columnMappers, null, null);
        schema.validate(metadata);
        schema.close();
    }

    @Test
    public void testToString() {

        ColumnMapperBuilder columnMapper1 = new ColumnMapperStringBuilder();
        ColumnMapperBuilder columnMapper2 = new ColumnMapperIntegerBuilder();

        Map<String, ColumnMapperBuilder> columnMappers = new HashMap<>();
        columnMappers.put("field1", columnMapper1);
        columnMappers.put("field2", columnMapper2);

        Schema schema = new Schema(columnMappers, null, null);
        assertNotNull(schema.toString());
        schema.close();
    }
}
