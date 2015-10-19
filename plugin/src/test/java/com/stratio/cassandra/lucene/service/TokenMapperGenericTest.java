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

package com.stratio.cassandra.lucene.service;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * Unit tests for {@link TokenMapperMurmur}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TokenMapperGenericTest {

    private static final RandomPartitioner partitioner = new RandomPartitioner();
    private static TokenMapperGeneric mapper;

    @BeforeClass
    public static void beforeClass() {
        Config.setClientMode(true);
        DatabaseDescriptor.setPartitioner(partitioner);
        mapper = new TokenMapperGeneric();
    }

    @Test
    public void testInstance() {
        assertEquals("Expected order preserving mapper", TokenMapperGeneric.class, TokenMapper.instance().getClass());
    }

    @Test
    public void testAddFields() {
        DecoratedKey key = partitioner.decorateKey(UTF8Type.instance.decompose("key"));
        Document document = new Document();
        mapper.addFields(document, key);
        IndexableField field = document.getField(TokenMapperGeneric.FIELD_NAME);
        assertNotNull("Field should be added", field);
        assertEquals("Hash value is wrong",
                     "[3c 6e b 8a 9c 15 22 4a 82 28 b9 a9 8c a1 53 1d]",
                     field.binaryValue().toString());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSortFields() throws IOException {
        List<SortField> sortFields = mapper.sortFields();
        assertNotNull("Sort fields should be not null", sortFields);
        assertEquals("Sort fields should contain a single element", 1, sortFields.size());
        for (SortField sortField : sortFields) {
            FieldComparatorSource comparatorSource = sortField.getComparatorSource();
            assertNotNull("Sort field comparator should be not null", comparatorSource);
            FieldComparator fieldComparator = comparatorSource.newComparator(TokenMapperGeneric.FIELD_NAME, 1, 0, true);
            BytesRef value1 = mapper.bytesRef(token("k1"));
            BytesRef value2 = mapper.bytesRef(token("k2"));
            fieldComparator.compareValues(value1, value2);
        }
    }

    @Test
    public void testQueryToken() {
        Token token = token("key");
        Query query = mapper.query(token);
        assertNotNull("Query should be not null", query);
        assertEquals("Hash value is wrong",
                     "_token_generic:[3c 6e b 8a 9c 15 22 4a 82 28 b9 a9 8c a1 53 1d]",
                     query.toString());
    }

    @Test
    public void testQueryRange() {
        Token token1 = token("key1");
        Token token2 = token("key2");
        Query query = mapper.query(token1, token2, true, false);
        assertNotNull("Query should be not null", query);
    }

    @Test
    public void testValue() {
        DecoratedKey key = decoratedKey("key");
        Token token = key.getToken();
        BytesRef value = mapper.bytesRef(token);
        assertNotNull("Value is wrong", value);
        assertEquals("Value is wrong", "[3c 6e b 8a 9c 15 22 4a 82 28 b9 a9 8c a1 53 1d]", value.toString());
    }

    @Test
    public void testToken() {
        DecoratedKey key = decoratedKey("key");
        Token expectedToken = key.getToken();
        BytesRef value = mapper.bytesRef(expectedToken);
        Token actualToken = mapper.token(value);
        assertNotNull("Token is wrong", actualToken);
        assertEquals("Token is wrong", expectedToken, actualToken);
    }

    private static DecoratedKey decoratedKey(String value) {
        return partitioner.decorateKey(UTF8Type.instance.decompose(value));
    }

    private static Token token(String value) {
        return decoratedKey(value).getToken();
    }
}
