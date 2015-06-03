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

import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class ColumnMapperUUIDTest {

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", null, null);
        assertEquals(ColumnMapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(ColumnMapper.DEFAULT_SORTED, mapper.isSorted());
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", false, true);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
    }

    @Test
    public void testValueNull() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test
    public void testValueUUIDRandom() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String parsed = mapper.base("test", "550e8400-e29b-41d4-a716-446655440000");
        assertEquals("04550e8400e29b41d4a716446655440000", parsed);
    }

    @Test
    public void testValueUUIDTimeBased() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String parsed = mapper.base("test", "c4c61dc4-89d7-11e4-b116-123b93f75cba");
        assertEquals("0101e489d7c4c61dc4c4c61dc489d711e4b116123b93f75cba", parsed);
    }

    @Test
    public void testValueStringRandom() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String parsed = mapper.base("test", "550e8400-e29b-41d4-a716-446655440000");
        assertEquals("04550e8400e29b41d4a716446655440000", parsed);
    }

    @Test
    public void testValueStringTimeBased() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String parsed = mapper.base("test", "c4c61dc4-89d7-11e4-b116-123b93f75cba");
        assertEquals("0101e489d7c4c61dc4c4c61dc489d711e4b116123b93f75cba", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        mapper.base("test", "550e840");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueInteger() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String parsed = mapper.base("test", 3);
        assertEquals("3", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueLong() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String parsed = mapper.base("test", 3l);
        assertEquals("3", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloat() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String parsed = mapper.base("test", 3.6f);
        assertEquals("3.6", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDouble() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String parsed = mapper.base("test", 3d);
        assertEquals("3.0", parsed);
    }

    @Test
    public void testIndexedField() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String base = mapper.base("name", "550e8400-e29b-41d4-a716-446655440000");
        Field field = mapper.indexedField("name", base);
        assertNotNull(field);
        assertEquals("name", field.name());
        assertEquals(base, field.stringValue());
        assertFalse(field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String base = mapper.base("name", "550e8400-e29b-41d4-a716-446655440000");
        Field field = mapper.sortedField("name", base, false);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        Field field = mapper.sortedField("name", "550e8400-e29b-41d4-a716-446655440000", true);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);
        String analyzer = mapper.getAnalyzer();
        assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"uuid\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNotNull(columnMapper);
        assertEquals(ColumnMapperUUID.class, columnMapper.getClass());
        assertEquals(ColumnMapper.DEFAULT_INDEXED, columnMapper.isIndexed());
        assertEquals(ColumnMapper.DEFAULT_SORTED, columnMapper.isSorted());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"uuid\", indexed:\"false\", sorted:\"true\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNotNull(columnMapper);
        assertEquals(ColumnMapperUUID.class, columnMapper.getClass());
        assertFalse(columnMapper.isIndexed());
        assertTrue(columnMapper.isSorted());
    }

    @Test
    public void testParseJSONEmpty() throws IOException {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNull(columnMapper);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }

    @Test
    public void testCompareDifferentTypes() {

        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);

        UUID uuidTimeBased = UUID.fromString("c4c61dc4-89d7-11e4-b116-123b93f75cba");
        UUID uuidRandom = UUID.fromString("c4c61dc4-89d7-41e4-b116-123b93f75cba");

        ByteBuffer bb1 = UUIDType.instance.decompose(uuidTimeBased);
        ByteBuffer bb2 = UUIDType.instance.decompose(uuidRandom);

        String s1 = mapper.base("uuidTimeBased", uuidTimeBased);
        String s2 = mapper.base("uuidRandom", uuidRandom);

        int nativeComparison = flatComparison(UUIDType.instance.compare(bb1, bb2));
        int mapperComparison = flatComparison(s1.compareTo(s2));

        assertEquals(nativeComparison, mapperComparison);
    }

    @Test
    public void testCompareTimeUUID() {

        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);

        UUID uuid1 = UUID.fromString("d9b602c0-89d8-11e4-b116-123b93f75cba");
        UUID uuid2 = UUID.fromString("d9b6ff0e-89d8-11e4-b116-123b93f75cba");

        ByteBuffer bb1 = UUIDType.instance.decompose(uuid1);
        ByteBuffer bb2 = UUIDType.instance.decompose(uuid2);

        String s1 = mapper.base("uuid1", uuid1);
        String s2 = mapper.base("uuid2", uuid2);

        int nativeComparison = flatComparison(UUIDType.instance.compare(bb1, bb2));
        int mapperComparison = flatComparison(s1.compareTo(s2));

        assertEquals(nativeComparison, mapperComparison);
    }

    @Test
    public void testCompareRandomUUID() throws InterruptedException {

        ColumnMapperUUID mapper = new ColumnMapperUUID("field", true, true);

        UUID uuid1 = UUID.fromString("5e9384d7-c72b-402a-aa13-2745f9b6b318");
        UUID uuid2 = UUID.fromString("eddfdc0d-76ee-4a5c-a155-3e5dd16ce1ae");

        ByteBuffer bb1 = UUIDType.instance.decompose(uuid1);
        ByteBuffer bb2 = UUIDType.instance.decompose(uuid2);

        String s1 = mapper.base("uuid1", uuid1);
        String s2 = mapper.base("uuid2", uuid2);

        int nativeComparison = flatComparison(UUIDType.instance.compare(bb1, bb2));
        int mapperComparison = flatComparison(s1.compareTo(s2));

        assertEquals(nativeComparison, mapperComparison);
    }

    @Test
    public void testSortTimeUUIDsAsGeneral() {
        List<UUID> uuids = toList("24f340bc-89da-11e4-b116-123b93f75cba",
                                  "24f34328-89da-11e4-b116-123b93f75cba",
                                  "24f34486-89da-11e4-b116-123b93f75cba",
                                  "24f3465c-89da-11e4-b116-123b93f75cba",
                                  "24f3481e-89da-11e4-b116-123b93f75cba",
                                  "24f3481e-89da-11e4-b116-123b93f75cba",
                                  "24f3495e-89da-11e4-b116-123b93f75cba",
                                  "24f34a8a-89da-11e4-b116-123b93f75cba",
                                  "24f34bb6-89da-11e4-b116-123b93f75cba",
                                  "24f34ce2-89da-11e4-b116-123b93f75cba",
                                  "24f34e0e-89da-11e4-b116-123b93f75cba");
        testSort(uuids, UUIDType.instance);
    }

    @Test
    public void testSortTimeUUIDsAsNative() {
        List<UUID> uuids = toList("24f340bc-89da-11e4-b116-123b93f75cba",
                                  "24f34328-89da-11e4-b116-123b93f75cba",
                                  "24f34486-89da-11e4-b116-123b93f75cba",
                                  "24f3465c-89da-11e4-b116-123b93f75cba",
                                  "24f3481e-89da-11e4-b116-123b93f75cba",
                                  "24f3481e-89da-11e4-b116-123b93f75cba",
                                  "24f3495e-89da-11e4-b116-123b93f75cba",
                                  "24f34a8a-89da-11e4-b116-123b93f75cba",
                                  "24f34bb6-89da-11e4-b116-123b93f75cba",
                                  "24f34ce2-89da-11e4-b116-123b93f75cba",
                                  "24f34e0e-89da-11e4-b116-123b93f75cba");
        testSort(uuids, TimeUUIDType.instance);
    }

    @Test
    public void testSortRandomUUIDs() {
        List<UUID> uuids = toList("520fdc7d-8d62-4c46-a22c-1f6c481f032f",
                                  "6a5a5f84-0482-408e-9600-6b7fafaaa9cb",
                                  "ece1ff82-c92c-4179-9e7f-0d6349810472",
                                  "6c211cca-fbf3-4777-b359-85440e10b1fa",
                                  "33b51b24-a2fe-4713-b881-d53acc970758",
                                  "33b51b24-a2fe-4713-b881-d53acc970758",
                                  "a156804e-7ec1-496a-af77-80b8576d6cda",
                                  "0c9510f1-b3de-404d-a38e-e6d73b5bd566",
                                  "cea36e37-de23-4875-912d-be1da52eef33",
                                  "055b32ee-8b26-4dc1-8e4f-70580f855349",
                                  "675b03f0-74bb-49b6-877f-562b6f306bea");
        testSort(uuids, UUIDType.instance);
    }

    @Test
    public void testSortMixedUUIDs() {
        List<UUID> uuids = toList("520fdc7d-8d62-4c46-a22c-1f6c481f032f",
                                  "6a5a5f84-0482-408e-9600-6b7fafaaa9cb",
                                  "ece1ff82-c92c-4179-9e7f-0d6349810472",
                                  "6c211cca-fbf3-4777-b359-85440e10b1fa",
                                  "33b51b24-a2fe-4713-b881-d53acc970758",
                                  "33b51b24-a2fe-4713-b881-d53acc970758",
                                  "a156804e-7ec1-496a-af77-80b8576d6cda",
                                  "0c9510f1-b3de-404d-a38e-e6d73b5bd566",
                                  "cea36e37-de23-4875-912d-be1da52eef33",
                                  "055b32ee-8b26-4dc1-8e4f-70580f855349",
                                  "675b03f0-74bb-49b6-877f-562b6f306bea",
                                  "24f340bc-89da-11e4-b116-123b93f75cba",
                                  "24f34328-89da-11e4-b116-123b93f75cba",
                                  "24f34486-89da-11e4-b116-123b93f75cba",
                                  "24f3465c-89da-11e4-b116-123b93f75cba",
                                  "24f3481e-89da-11e4-b116-123b93f75cba",
                                  "24f3481e-89da-11e4-b116-123b93f75cba",
                                  "24f3495e-89da-11e4-b116-123b93f75cba",
                                  "24f34a8a-89da-11e4-b116-123b93f75cba",
                                  "24f34bb6-89da-11e4-b116-123b93f75cba",
                                  "24f34ce2-89da-11e4-b116-123b93f75cba",
                                  "24f34e0e-89da-11e4-b116-123b93f75cba");
        testSort(uuids, UUIDType.instance);
    }

    private void testSort(List<UUID> uuids, final AbstractType<UUID> type) {

        Collections.shuffle(uuids);

        List<UUID> expectedList = new ArrayList<>(uuids);
        Collections.sort(expectedList, new Comparator<UUID>() {
            @Override
            public int compare(UUID o1, UUID o2) {
                return type.compare(type.decompose(o1), type.decompose(o2));
            }
        });

        List<UUID> actualList = new ArrayList<>(uuids);
        Collections.sort(actualList, new Comparator<UUID>() {
            @Override
            public int compare(UUID o1, UUID o2) {
                String s1 = ColumnMapperUUID.serialize(o1);
                String s2 = ColumnMapperUUID.serialize(o2);
                return s1.compareTo(s2);
            }
        });

        assertEquals(expectedList.size(), actualList.size());
        for (int i = 0; i < expectedList.size(); i++) {
            UUID expectedUUID = expectedList.get(i);
            UUID actualUUID = actualList.get(i);
            assertEquals(expectedUUID, actualUUID);
        }
    }

    private int flatComparison(int comp) {
        if (comp == 0) {
            return 0;
        } else if (comp > 0) {
            return 1;
        } else {
            return -1;
        }
    }

    private List<UUID> toList(String... uuids) {
        List<UUID> result = new ArrayList<>(uuids.length);
        for (String s : uuids) {
            result.add(UUID.fromString(s));
        }
        return result;
    }

    @Test
    public void testToString() {
        ColumnMapperUUID mapper = new ColumnMapperUUID("field", false, false);
        assertEquals("ColumnMapperUUID{indexed=false, sorted=false}", mapper.toString());
    }
}
