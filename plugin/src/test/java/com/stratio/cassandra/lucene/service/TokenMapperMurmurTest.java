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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
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

    @Test
    public void testAddFields() {
        Murmur3Partitioner partitioner = new Murmur3Partitioner();
        TokenMapperMurmur mapper = new TokenMapperMurmur();
        DecoratedKey key = partitioner.decorateKey(UTF8Type.instance.decompose("key"));
        Document document = new Document();
        mapper.addFields(document, key);
        IndexableField field = document.getField(TokenMapperMurmur.FIELD_NAME);
        assertNotNull("Field should be added", field);
        assertEquals("Hash value is wrong", -6847573755651342660L, field.numericValue());
    }

    @Test
    public void testSortFields() {
        TokenMapperMurmur mapper = new TokenMapperMurmur();
        List<SortField> sortFields = mapper.sortFields();
        assertNotNull("Sort fields should be not null", sortFields);
        assertEquals("Sort fields should contain a single element", 1, sortFields.size());
    }

    @Test
    public void testQueryToken() {
        Murmur3Partitioner partitioner = new Murmur3Partitioner();
        DecoratedKey key = partitioner.decorateKey(UTF8Type.instance.decompose("key"));
        Token token = key.getToken();
        TokenMapperMurmur mapper = new TokenMapperMurmur();
        Query query = mapper.query(token);
        assertNotNull("Query should be not null", query);
        assertEquals("Hash value is wrong",
                     "_token_murmur:[-6847573755651342660 TO -6847573755651342660]",
                     query.toString());
    }

    @Test
    public void testValue() {
        Murmur3Partitioner partitioner = new Murmur3Partitioner();
        DecoratedKey key = partitioner.decorateKey(UTF8Type.instance.decompose("key"));
        Token token = key.getToken();
        long value = TokenMapperMurmur.value(token);
        assertEquals("Hash value is wrong", -6847573755651342660L, value);
    }
}
