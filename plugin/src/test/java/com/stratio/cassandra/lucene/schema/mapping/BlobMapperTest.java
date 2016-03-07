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

package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.mapping.builder.BlobMapperBuilder;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.blobMapper;
import static org.junit.Assert.*;

public class BlobMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        BlobMapper mapper = new BlobMapperBuilder().build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertEquals("Indexed is not set to default value", Mapper.DEFAULT_INDEXED, mapper.indexed);
        assertEquals("Sorted is not set to default value", Mapper.DEFAULT_SORTED, mapper.sorted);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
    }

    @Test
    public void testConstructorWithAllArgs() {
        BlobMapper mapper = new BlobMapperBuilder().indexed(false)
                                                   .sorted(true)
                                                   .validated(true)
                                                   .column("column")
                                                   .build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertFalse("Indexed is not properly set", mapper.indexed);
        assertTrue("Sorted is not properly set", mapper.sorted);
        assertTrue("Validated is not properly set", mapper.validated);
        assertEquals("Column is not properly set", "column", mapper.column);
    }

    @Test
    public void testJsonSerialization() {
        BlobMapperBuilder builder = new BlobMapperBuilder().indexed(false).sorted(true).column("column");
        testJson(builder, "{type:\"bytes\",indexed:false,sorted:true,column:\"column\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        BlobMapperBuilder builder = new BlobMapperBuilder();
        testJson(builder, "{type:\"bytes\"}");
    }

    @Test
    public void testValueNull() {
        BlobMapper mapper = blobMapper().build("field");
        assertNull("Base value is not properly parsed", mapper.base("test", null));
    }

    @Test(expected = IndexException.class)
    public void testValueInteger() {
        BlobMapper mapper = blobMapper().build("field");
        mapper.base("test", 3);
    }

    @Test(expected = IndexException.class)
    public void testValueLong() {
        BlobMapper mapper = blobMapper().build("field");
        mapper.base("test", 3l);
    }

    @Test(expected = IndexException.class)
    public void testValueFloat() {
        BlobMapper mapper = blobMapper().build("field");
        mapper.base("test", 3.5f);
    }

    @Test(expected = IndexException.class)
    public void testValueDouble() {
        BlobMapper mapper = blobMapper().build("field");
        mapper.base("test", 3.6d);
    }

    @Test(expected = IndexException.class)
    public void testValueUUID() {
        BlobMapper mapper = blobMapper().build("field");
        mapper.base("test", UUID.randomUUID());
    }

    @Test(expected = IndexException.class)
    public void testValueStringInvalid() {
        BlobMapper mapper = blobMapper().build("field");
        mapper.base("test", "Hello");
    }

    @Test
    public void testValueStringLowerCaseWithoutPrefix() {
        BlobMapper mapper = blobMapper().build("field");
        String parsed = mapper.base("test", "f1");
        assertEquals("Base value is not properly parsed", "f1", parsed);
    }

    @Test
    public void testValueStringUpperCaseWithoutPrefix() {
        BlobMapper mapper = blobMapper().build("field");
        String parsed = mapper.base("test", "F1");
        assertEquals("Base value is not properly parsed", "f1", parsed);
    }

    @Test
    public void testValueStringMixedCaseWithoutPrefix() {
        BlobMapper mapper = blobMapper().build("field");
        String parsed = mapper.base("test", "F1a2B3");
        assertEquals("Base value is not properly parsed", "f1a2b3", parsed);
    }

    @Test
    public void testValueStringLowerCaseWithPrefix() {
        BlobMapper mapper = blobMapper().build("field");
        String parsed = mapper.base("test", "0xf1");
        assertEquals("Base value is not properly parsed", "f1", parsed);
    }

    @Test
    public void testValueStringUpperCaseWithPrefix() {
        BlobMapper mapper = blobMapper().build("field");
        String parsed = mapper.base("test", "0xF1");
        assertEquals("Base value is not properly parsed", "f1", parsed);
    }

    @Test
    public void testValueStringMixedCaseWithPrefix() {
        BlobMapper mapper = blobMapper().build("field");
        String parsed = mapper.base("test", "0xF1a2B3");
        assertEquals("Base value is not properly parsed", "f1a2b3", parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringOdd() {
        BlobMapper mapper = blobMapper().build("field");
        mapper.base("test", "f");
    }

    @Test
    public void testValueByteBuffer() {
        BlobMapper mapper = blobMapper().build("field");
        ByteBuffer bb = ByteBufferUtil.hexToBytes("f1");
        String parsed = mapper.base("test", bb);
        assertEquals("Base value is not properly parsed", "f1", parsed);
    }

    @Test
    public void testValueBytes() {
        BlobMapper mapper = blobMapper().build("field");
        byte[] bytes = Hex.hexToBytes("f1");
        String parsed = mapper.base("test", bytes);
        assertEquals("Base value is not properly parsed", "f1", parsed);
    }

    @Test
    public void testIndexedField() {
        BlobMapper mapper = blobMapper().indexed(true).build("field");
        String base = mapper.base("name", "f1B2");
        Field field = mapper.indexedField("name", base);
        assertNotNull("Indexed field is not created", field);
        assertEquals("Indexed field value is wrong", base, field.stringValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field type is wrong", false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        BlobMapper mapper = blobMapper().sorted(true).build("field");
        String base = mapper.base("name", "f1B2");
        Field field = mapper.sortedField("name", base);
        assertNotNull("Sorted field is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        BlobMapper mapper = blobMapper().build("field");
        assertEquals("Analyzer must be keyword", Mapper.KEYWORD_ANALYZER, mapper.analyzer);
    }

    @Test
    public void testToString() {
        BlobMapper mapper = blobMapper().indexed(false).sorted(true).validated(true).build("field");
        assertEquals("Method #toString is wrong",
                     "BlobMapper{field=field, indexed=false, sorted=true, validated=true, column=field}",
                     mapper.toString());
    }
}
