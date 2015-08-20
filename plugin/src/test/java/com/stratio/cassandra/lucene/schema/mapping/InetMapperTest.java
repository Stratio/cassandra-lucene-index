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
import com.stratio.cassandra.lucene.schema.mapping.builder.InetMapperBuilder;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import static org.junit.Assert.*;

public class InetMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        InetMapper mapper = new InetMapperBuilder().build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertEquals("Indexed is not set to default value", Mapper.DEFAULT_INDEXED, mapper.indexed);
        assertEquals("Sorted is not set to default value", Mapper.DEFAULT_SORTED, mapper.sorted);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
    }

    @Test
    public void testConstructorWithAllArgs() {
        InetMapper mapper = new InetMapperBuilder().indexed(false).sorted(true).column("column").build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertFalse("Indexed is not properly set", mapper.indexed);
        assertTrue("Sorted is not properly set", mapper.sorted);
        assertEquals("Column is not properly set", "column", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("column"));
    }

    @Test
    public void testJsonSerialization() {
        InetMapperBuilder builder = new InetMapperBuilder().indexed(false).sorted(true).column("column");
        testJson(builder, "{type:\"inet\",indexed:false,sorted:true,column:\"column\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        InetMapperBuilder builder = new InetMapperBuilder();
        testJson(builder, "{type:\"inet\"}");
    }

    @Test()
    public void testValueNull() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        String parsed = mapper.base("test", null);
        assertNull("Base for nulls is wrong", parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueInteger() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        mapper.base("test", 3);
    }

    @Test(expected = IndexException.class)
    public void testValueLong() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        mapper.base("test", 3l);
    }

    @Test(expected = IndexException.class)
    public void testValueFloat() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        mapper.base("test", 3.5f);
    }

    @Test(expected = IndexException.class)
    public void testValueDouble() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        mapper.base("test", 3.6d);
    }

    @Test(expected = IndexException.class)
    public void testValueUUID() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        mapper.base("test", UUID.randomUUID());
    }

    @Test(expected = IndexException.class)
    public void testValueStringInvalid() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        mapper.base("test", "Hello");
    }

    @Test
    public void testValueStringV4WithoutZeros() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        String parsed = mapper.base("test", "192.168.0.1");
        assertEquals("Base for strings is wrong", "192.168.0.1", parsed);
    }

    @Test
    public void testValueStringV4WithZeros() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        String parsed = mapper.base("test", "192.168.000.001");
        assertEquals("Base for strings is wrong", "192.168.0.1", parsed);
    }

    @Test
    public void testValueStringV6WithoutZeros() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        String parsed = mapper.base("test", "2001:db8:2de:0:0:0:0:e13");
        assertEquals("Base for strings is wrong", "2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testValueStringV6WithZeros() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        String parsed = mapper.base("test", "2001:0db8:02de:0000:0000:0000:0000:0e13");
        assertEquals("Base for strings is wrong", "2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testValueStringV6Compact() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        String parsed = mapper.base("test", "2001:DB8:2de::0e13");
        assertEquals("Base for strings is wrong", "2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testValueInetV4() throws UnknownHostException {
        InetMapper mapper = new InetMapper("field", null, null, null);
        InetAddress inet = InetAddress.getByName("192.168.0.13");
        String parsed = mapper.base("test", inet);
        assertEquals("Base for strings is wrong", "192.168.0.13", parsed);
    }

    @Test
    public void testValueInetV6() throws UnknownHostException {
        InetMapper mapper = new InetMapper("field", null, null, null);
        InetAddress inet = InetAddress.getByName("2001:db8:2de:0:0:0:0:e13");
        String parsed = mapper.base("test", inet);
        assertEquals("Base for strings is wrong", "2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testIndexedField() {
        InetMapper mapper = new InetMapper("field", null, true, true);
        Field field = mapper.indexedField("name", "192.168.0.13");
        assertNotNull("Indexed field is not created", field);
        assertEquals("Indexed field value is wrong", "192.168.0.13", field.stringValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field type is wrong", false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        InetMapper mapper = new InetMapper("field", null, true, true);
        Field field = mapper.sortedField("name", "192.168.0.13");
        assertNotNull("Sorted field is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        InetMapper mapper = new InetMapper("field", null, null, null);
        String analyzer = mapper.analyzer;
        assertEquals("Analyzer must be null", Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testToString() {
        InetMapper mapper = new InetMapper("field", null, false, false);
        assertEquals("Method #toString is wrong",
                     "InetMapper{field=field, indexed=false, sorted=false, column=field}",
                     mapper.toString());
    }
}
