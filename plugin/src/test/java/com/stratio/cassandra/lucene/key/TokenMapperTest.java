/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.key;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TokenMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TokenMapperTest {

    private static final Murmur3Partitioner partitioner = new Murmur3Partitioner();
    private static TokenMapper mapper;

    private void setMurmur3Partitioneer() {
        Config.setClientMode(true);
        DatabaseDescriptor.setPartitioner(partitioner);
        mapper = new TokenMapper();
    }

    @BeforeClass
    public static void beforeClass() {
        Config.setClientMode(true);
        DatabaseDescriptor.setPartitioner(partitioner);
        mapper = new TokenMapper();
    }

    @Test
    public void testConstructorWithoutValidPartitioner1() {
        IPartitioner partitioner = new ByteOrderedPartitioner();
        DatabaseDescriptor.setPartitioner(partitioner);

        try {
            TokenMapper tm = new TokenMapper();
        } catch (IndexException iE) {
            assertEquals(
                    "Building a TokenMapper with a ByteOrdered Partitioner must return an IndexException with an exact message: \"Only Murmur3 partitioner is supported\"",
                    "Only Murmur3 partitioner is supported",
                    iE.getMessage());
        }
    }

    @Test
    public void testConstructorWithoutValidPartitioner2() {
        IPartitioner partitioner = new OrderPreservingPartitioner();
        DatabaseDescriptor.setPartitioner(partitioner);
        try {
            TokenMapper tm = new TokenMapper();
        } catch (IndexException iE) {
            assertEquals(
                    "Building a TokenMapper with a OrderPreserving Partitioner must return an IndexException with an exact message: \"Only Murmur3 partitioner is supported\"",
                    "Only Murmur3 partitioner is supported",
                    iE.getMessage());
        }
    }

    @Test
    public void testConstructorWithoutValidPartitioner3() {
        IPartitioner partitioner = new RandomPartitioner();
        DatabaseDescriptor.setPartitioner(partitioner);
        try {
            TokenMapper tm = new TokenMapper();
        } catch (IndexException iE) {
            assertEquals(
                    "Building a TokenMapper with a RandomPartitioner Partitioner must return an IndexException with an exact message: \"Only Murmur3 partitioner is supported\"",
                    "Only Murmur3 partitioner is supported",
                    iE.getMessage());
        }
    }

    @Test
    public void testConstructorWithoutValidPartitioner4() {
        IPartitioner partitioner = new LocalPartitioner(UTF8Type.instance);
        DatabaseDescriptor.setPartitioner(partitioner);
        try {
            TokenMapper tm = new TokenMapper();
        } catch (IndexException iE) {
            assertEquals(
                    "Building a TokenMapper with a LocalPartitioner Partitioner must return an IndexException with an exact message: \"Only Murmur3 partitioner is supported\"",
                    "Only Murmur3 partitioner is supported",
                    iE.getMessage());
        }
    }

    @Test
    public void testConstructorWithValidPartitioner() {
        IPartitioner partitioner = new Murmur3Partitioner();
        DatabaseDescriptor.setPartitioner(partitioner);
        new TokenMapper();
    }

    @Test
    public void testQueryRangeNullLower() {
        setMurmur3Partitioneer();
        Token token = partitioner.getMinimumToken();
        Query query = mapper.query(null, token, false, true);
        assertNotNull("Query should be not null", query);
        assertEquals("Query must be delegated", DocValuesRangeQuery.class, query.getClass());
    }

    @Test
    public void testQueryRangeNullUpper() {
        setMurmur3Partitioneer();
        Token token = partitioner.getMinimumToken();
        Query query = mapper.query(token, null, false, true);
        assertNotNull("Query should be not null", query);
        assertEquals("Query must be delegated", DocValuesRangeQuery.class, query.getClass());
    }

    @Test
    public void testQueryRangeNullBoth() {
        setMurmur3Partitioneer();
        Query query = mapper.query(null, null, false, true);
        assertNotNull("Query should be not null", query);
        assertEquals("Query must be delegated", DocValuesRangeQuery.class, query.getClass());
    }

    @Test
    public void testQueryRangeMinimumLower() {
        setMurmur3Partitioneer();
        Token token1 = partitioner.getMinimumToken();
        Token token2 = token("key2");
        Query query = mapper.query(token1, token2, true, false);
        assertNotNull("Query should be not null", query);
        assertEquals("Hash value is wrong", DocValuesRangeQuery.class, query.getClass());
    }

    @Test
    public void testQueryRangeMinimumUpper() {
        setMurmur3Partitioneer();
        Token token1 = token("key1");
        Token token2 = partitioner.getMinimumToken();
        Query query = mapper.query(token1, token2, false, true);
        assertNotNull("Query should be not null", query);
        assertEquals("Hash value is wrong", DocValuesRangeQuery.class, query.getClass());
    }

    @Test
    public void testQueryRangeMinimumBoth() {
        setMurmur3Partitioneer();
        Token token1 = partitioner.getMinimumToken();
        Token token2 = partitioner.getMinimumToken();
        Query query = mapper.query(token1, token2, false, true);
        assertNull("Query should be null", query);
    }

    @Test
    public void testQueryRangeMinimumBothIncluded() {
        setMurmur3Partitioneer();
        Token token1 = partitioner.getMinimumToken();
        Token token2 = partitioner.getMinimumToken();
        Query query = mapper.query(token1, token2, true, true);
        assertNull("Query should be null", query);
    }

    @Test
    public void testQueryRangeMinimumNotIncluded() {
        setMurmur3Partitioneer();
        Token token1 = partitioner.getMinimumToken();
        Token token2 = partitioner.getMinimumToken();
        Query query = mapper.query(token1, token2, false, false);
        assertNotNull("Query should be not null", query);
        assertEquals("Hash value is wrong", DocValuesRangeQuery.class, query.getClass());
    }

    @Test
    public void testComparator() {
        CFMetaData metaData = CFMetaData.denseCFMetaData("keyspace", "cf", UTF8Type.instance);
        Row row1 = new Row(decoratedKey("k1"), ArrayBackedSortedColumns.factory.create(metaData));
        Row row2 = new Row(decoratedKey("k2"), ArrayBackedSortedColumns.factory.create(metaData));
        Comparator<Row> comparator = mapper.comparator();
        int comparison = comparator.compare(row1, row2);
        assertEquals("Comparison is wrong", -1, comparison);
    }

    @Test
    public void testIncludeStart() {
        RowPosition position = Mockito.mock(RowPosition.class);
        when(position.kind()).thenReturn(RowPosition.Kind.MAX_BOUND);
        assertFalse("Include start is wrong", mapper.includeStart(position));
        when(position.kind()).thenReturn(RowPosition.Kind.MIN_BOUND);
        assertTrue("Include start is wrong", mapper.includeStart(position));
        when(position.kind()).thenReturn(RowPosition.Kind.ROW_KEY);
        assertTrue("Include start is wrong", mapper.includeStart(position));
    }

    @Test
    public void testIncludeStop() {
        RowPosition position = Mockito.mock(RowPosition.class);
        when(position.kind()).thenReturn(RowPosition.Kind.MAX_BOUND);
        assertTrue("Include stop is wrong", mapper.includeStop(position));
        when(position.kind()).thenReturn(RowPosition.Kind.MIN_BOUND);
        assertFalse("Include stop is wrong", mapper.includeStop(position));
        when(position.kind()).thenReturn(RowPosition.Kind.ROW_KEY);
        assertTrue("Include stop is wrong", mapper.includeStop(position));
    }

    @Test
    public void testAddFields() {
        setMurmur3Partitioneer();
        DecoratedKey key = partitioner.decorateKey(UTF8Type.instance.decompose("key"));
        Document document = new Document();
        mapper.addFields(document, key);
        IndexableField field = document.getField(TokenMapper.FIELD_NAME);
        assertNotNull("Field should be added", field);
        assertEquals("Hash value is wrong", -6847573755651342660L, field.numericValue());
    }

    @Test
    public void testSortFields() {
        setMurmur3Partitioneer();
        SortField sortField = mapper.sortField();
        assertNotNull("Sort fields should be not null", sortField);
    }

    @Test
    public void testQueryToken() {
        setMurmur3Partitioneer();
        Token token = token("key");
        Query query = mapper.query(token);
        assertNotNull("Query should be not null", query);
        assertEquals("Hash value is wrong",
                     "_token:[-6847573755651342660 TO -6847573755651342660]",
                     query.toString());
    }

    @Test
    public void testQueryRange() {
        setMurmur3Partitioneer();
        Token token1 = token("key1");
        Token token2 = token("key2");
        Query query = mapper.query(token1, token2, true, false);
        assertNotNull("Query should be not null", query);
        assertEquals("Hash value is wrong",
                     "_token:[1573573083296714675 TO 8482869187405483569}",
                     query.toString());
    }

    @Test
    public void testValue() {
        DecoratedKey key = decoratedKey("key");
        Token token = key.getToken();
        long value = TokenMapper.value(token);
        assertEquals("Hash value is wrong", -6847573755651342660L, value);
    }

    private static final List<Murmur3Partitioner.LongToken>
            TOKENS
            = Arrays.asList(new Murmur3Partitioner.LongToken(Long.MIN_VALUE),
                            new Murmur3Partitioner.LongToken(-12345),
                            new Murmur3Partitioner.LongToken(-123),
                            new Murmur3Partitioner.LongToken(0),
                            new Murmur3Partitioner.LongToken(123),
                            new Murmur3Partitioner.LongToken(12345),
                            new Murmur3Partitioner.LongToken(Long.MAX_VALUE));

    private static final List<String> STRINGS = Arrays.asList("0000000000000000",
                                                              "7fffffffffffcfc7",
                                                              "7fffffffffffff85",
                                                              "8000000000000000",
                                                              "800000000000007b",
                                                              "8000000000003039",
                                                              "ffffffffffffffff");

    @Test
    public void testToCollated() {
        List<String> strings = new ArrayList<String>();
        for (int i = 0; i < STRINGS.size(); i++) {
            strings.add(UTF8Type.instance.compose(TokenMapper.toCollated(TOKENS.get(i))));
        }
        assertArrayEquals("TokenMapper.toCollated is wrong", STRINGS.toArray(), strings.toArray());
    }

    @Test
    public void testFromCollated() {
        List<Token> tokens = new ArrayList<Token>();
        for (int i = 0; i < STRINGS.size(); i++) {
            tokens.add(TokenMapper.fromCollated(UTF8Type.instance.decompose(STRINGS.get(i))));
        }
        assertArrayEquals("TokenMapper.toCollated is wrong", TOKENS.toArray(), tokens.toArray());
    }

    private static DecoratedKey decoratedKey(String value) {
        return partitioner.decorateKey(UTF8Type.instance.decompose(value));
    }

    private static Token token(String value) {
        return decoratedKey(value).getToken();
    }

}
