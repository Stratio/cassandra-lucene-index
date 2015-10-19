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
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * Unit tests for {@link TokenMapperMurmur}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TokenMapperMurmurTest {

    private static final Murmur3Partitioner partitioner = new Murmur3Partitioner();
    private static final TokenMapperMurmur mapper = new TokenMapperMurmur();

    @BeforeClass
    public static void beforeClass() {
        Config.setClientMode(true);
        DatabaseDescriptor.setPartitioner(partitioner);
    }

    @Test
    public void testInstance() {
        assertEquals("Expected Murmur mapper", TokenMapperMurmur.class, TokenMapper.instance().getClass());
    }

    @Test
    public void testAddFields() {
        DecoratedKey key = partitioner.decorateKey(UTF8Type.instance.decompose("key"));
        Document document = new Document();
        mapper.addFields(document, key);
        IndexableField field = document.getField(TokenMapperMurmur.FIELD_NAME);
        assertNotNull("Field should be added", field);
        assertEquals("Hash value is wrong", -6847573755651342660L, field.numericValue());
    }

    @Test
    public void testSortFields() {
        List<SortField> sortFields = mapper.sortFields();
        assertNotNull("Sort fields should be not null", sortFields);
        assertEquals("Sort fields should contain a single element", 1, sortFields.size());
    }

    @Test
    public void testQueryToken() {
        Token token = token("key");
        Query query = mapper.query(token);
        assertNotNull("Query should be not null", query);
        assertEquals("Hash value is wrong",
                     "_token_murmur:[-6847573755651342660 TO -6847573755651342660]",
                     query.toString());
    }

    @Test
    public void testQueryRange() {
        Token token1 = token("key1");
        Token token2 = token("key2");
        Query query = mapper.query(token1, token2, true, false);
        assertNotNull("Query should be not null", query);
        assertEquals("Hash value is wrong",
                     "_token_murmur:[1573573083296714675 TO 8482869187405483569}",
                     query.toString());
    }

    @Test
    public void testValue() {
        DecoratedKey key = decoratedKey("key");
        Token token = key.getToken();
        long value = TokenMapperMurmur.value(token);
        assertEquals("Hash value is wrong", -6847573755651342660L, value);
    }

    private static DecoratedKey decoratedKey(String value) {
        return partitioner.decorateKey(UTF8Type.instance.decompose(value));
    }

    private static Token token(String value) {
        return decoratedKey(value).getToken();
    }
}

