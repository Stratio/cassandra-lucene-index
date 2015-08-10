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
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SchemaTest {

    @Test
    public void testGetDefaultAnalyzer() {
        Map<String, Mapper> mappers = new HashMap<>();
        Schema schema = new Schema(new EnglishAnalyzer(), mappers, null);
        Analyzer analyzer = schema.getDefaultAnalyzer();
        assertEquals("Expected english analyzer", EnglishAnalyzer.class, analyzer.getClass());
        schema.close();
    }

    @Test
    public void testGetDefaultAnalyzerNotSpecified() {
        Map<String, Mapper> mappers = new HashMap<>();
        Schema schema = new Schema(null, mappers, null);
        Analyzer analyzer = schema.getDefaultAnalyzer();
        assertEquals("Expected default analyzer", PreBuiltAnalyzers.DEFAULT.get().getClass(), analyzer.getClass());
        schema.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerNotExistent() {
        Map<String, Mapper> mappers = new HashMap<>();
        Schema schema = new Schema(new EnglishAnalyzer(), mappers, null);
        schema.getAnalyzer("custom");
        schema.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerNull() {
        Map<String, Mapper> mappers = new HashMap<>();
        Schema schema = new Schema(new EnglishAnalyzer(), mappers, null);
        schema.getAnalyzer(null);
        schema.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerEmpty() {
        Map<String, Mapper> mappers = new HashMap<>();
        Schema schema = new Schema(new EnglishAnalyzer(), mappers, null);
        schema.getAnalyzer(" \t");
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

        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();
        schema.validate(metadata);
        schema.close();
    }

    @Test
    public void testToString() {

        Schema schema = schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();
        assertNotNull("Expected not null schema", schema.toString());
        schema.close();
    }
}
