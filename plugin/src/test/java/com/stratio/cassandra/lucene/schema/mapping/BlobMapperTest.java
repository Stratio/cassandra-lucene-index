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

import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.*;

public class BlobMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
    }

    @Test
    public void testConstructorWithAllArgs() {
        BlobMapper mapper = new BlobMapper("field", false, true);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
    }

    @Test()
    public void testValueNull() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        String parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueInteger() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        mapper.base("test", 3);
    }

    @Test(expected = IndexException.class)
    public void testValueLong() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        mapper.base("test", 3l);
    }

    @Test(expected = IndexException.class)
    public void testValueFloat() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        mapper.base("test", 3.5f);
    }

    @Test(expected = IndexException.class)
    public void testValueDouble() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        mapper.base("test", 3.6d);
    }

    @Test(expected = IndexException.class)
    public void testValueUUID() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        mapper.base("test", UUID.randomUUID());
    }

    @Test(expected = IndexException.class)
    public void testValueStringInvalid() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        mapper.base("test", "Hello");
    }

    @Test
    public void testValueStringLowerCaseWithoutPrefix() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        String parsed = mapper.base("test", "f1");
        assertEquals("f1", parsed);
    }

    @Test
    public void testValueStringUpperCaseWithoutPrefix() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        String parsed = mapper.base("test", "F1");
        assertEquals("f1", parsed);
    }

    @Test
    public void testValueStringMixedCaseWithoutPrefix() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        String parsed = mapper.base("test", "F1a2B3");
        assertEquals("f1a2b3", parsed);
    }

    @Test
    public void testValueStringLowerCaseWithPrefix() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        String parsed = mapper.base("test", "0xf1");
        assertEquals("f1", parsed);
    }

    @Test
    public void testValueStringUpperCaseWithPrefix() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        String parsed = mapper.base("test", "0xF1");
        assertEquals("f1", parsed);
    }

    @Test
    public void testValueStringMixedCaseWithPrefix() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        String parsed = mapper.base("test", "0xF1a2B3");
        assertEquals("f1a2b3", parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringOdd() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        mapper.base("test", "f");
    }

    @Test
    public void testValueByteBuffer() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        ByteBuffer bb = ByteBufferUtil.hexToBytes("f1");
        String parsed = mapper.base("test", bb);
        assertEquals("f1", parsed);
    }

    @Test
    public void testValueBytes() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        byte[] bytes = Hex.hexToBytes("f1");
        String parsed = mapper.base("test", bytes);
        assertEquals("f1", parsed);
    }

    @Test
    public void testIndexedField() {
        BlobMapper mapper = new BlobMapper("field", true, null);
        String base = mapper.base("name", "f1B2");
        Field field = mapper.indexedField("name", base);
        assertNotNull(field);
        assertNotNull(field);
        assertEquals(base, field.stringValue());
        assertEquals("name", field.name());
        assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        BlobMapper mapper = new BlobMapper("field", true, false);
        String base = mapper.base("name", "f1B2");
        Field field = mapper.sortedField("name", base);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        BlobMapper mapper = new BlobMapper("field", null, null);
        String analyzer = mapper.getAnalyzer();
        assertEquals(Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testToString() {
        BlobMapper mapper = new BlobMapper("field", false, false);
        assertEquals("BlobMapper{indexed=false, sorted=false}", mapper.toString());
    }
}
