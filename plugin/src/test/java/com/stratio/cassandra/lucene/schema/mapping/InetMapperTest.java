/*
 * Copyright 2014, Stratio.
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
package com.stratio.cassandra.lucene.schema.mapping;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import static org.junit.Assert.*;

public class InetMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        InetMapper mapper = new InetMapper("field", null, null);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
    }

    @Test
    public void testConstructorWithAllArgs() {
        InetMapper mapper = new InetMapper("field", false, true);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
    }

    @Test()
    public void testValueNull() {
        InetMapper mapper = new InetMapper("field", null, null);
        String parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueInteger() {
        InetMapper mapper = new InetMapper("field", null, null);
        mapper.base("test", 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueLong() {
        InetMapper mapper = new InetMapper("field", null, null);
        mapper.base("test", 3l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloat() {
        InetMapper mapper = new InetMapper("field", null, null);
        mapper.base("test", 3.5f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDouble() {
        InetMapper mapper = new InetMapper("field", null, null);
        mapper.base("test", 3.6d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueUUID() {
        InetMapper mapper = new InetMapper("field", null, null);
        mapper.base("test", UUID.randomUUID());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        InetMapper mapper = new InetMapper("field", null, null);
        mapper.base("test", "Hello");
    }

    @Test
    public void testValueStringV4WithoutZeros() {
        InetMapper mapper = new InetMapper("field", null, null);
        String parsed = mapper.base("test", "192.168.0.1");
        assertEquals("192.168.0.1", parsed);
    }

    @Test
    public void testValueStringV4WithZeros() {
        InetMapper mapper = new InetMapper("field", null, null);
        String parsed = mapper.base("test", "192.168.000.001");
        assertEquals("192.168.0.1", parsed);
    }

    @Test
    public void testValueStringV6WithoutZeros() {
        InetMapper mapper = new InetMapper("field", null, null);
        String parsed = mapper.base("test", "2001:db8:2de:0:0:0:0:e13");
        assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testValueStringV6WithZeros() {
        InetMapper mapper = new InetMapper("field", null, null);
        String parsed = mapper.base("test", "2001:0db8:02de:0000:0000:0000:0000:0e13");
        assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testValueStringV6Compact() {
        InetMapper mapper = new InetMapper("field", null, null);
        String parsed = mapper.base("test", "2001:DB8:2de::0e13");
        assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testValueInetV4() throws UnknownHostException {
        InetMapper mapper = new InetMapper("field", null, null);
        InetAddress inet = InetAddress.getByName("192.168.0.13");
        String parsed = mapper.base("test", inet);
        assertEquals("192.168.0.13", parsed);
    }

    @Test
    public void testValueInetV6() throws UnknownHostException {
        InetMapper mapper = new InetMapper("field", null, null);
        InetAddress inet = InetAddress.getByName("2001:db8:2de:0:0:0:0:e13");
        String parsed = mapper.base("test", inet);
        assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testIndexedField() {
        InetMapper mapper = new InetMapper("field", true, true);
        Field field = mapper.indexedField("name", "192.168.0.13");
        assertNotNull(field);
        assertEquals("192.168.0.13", field.stringValue());
        assertEquals("name", field.name());
        assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        InetMapper mapper = new InetMapper("field", true, true);
        Field field = mapper.sortedField("name", "192.168.0.13");
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        InetMapper mapper = new InetMapper("field", null, null);
        String analyzer = mapper.getAnalyzer();
        assertEquals(Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testToString() {
        InetMapper mapper = new InetMapper("field", false, false);
        assertEquals("InetMapper{indexed=false, sorted=false}", mapper.toString());
    }
}
