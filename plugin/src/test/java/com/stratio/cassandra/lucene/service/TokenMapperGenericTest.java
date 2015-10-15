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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.junit.Test;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class TokenMapperGenericTest {

    @Test
    public void testCRUD() {

/* need to mock static  DatabaseDescriptor.getPartitioner()
        TokenMapperGeneric tokenMapperMurmur = new TokenMapperGeneric();
        assertNotNull("TokenMapperGeneric constructor returning null", tokenMapperMurmur);
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        Document doc = new Document();

        tokenMapperMurmur.addFields(doc, decoratedKey);
        Field field = (Field) doc.getField("_token_murmur");
        LongField longField = (LongField) field;
        assertNotNull("tokenMapperMurmur addFields to Document must add al least one Field to Doc", field);
        assertEquals("tokenMapperMurmur addFields to Document must include a LongField with name and value ", field
                .name(), "_token_murmur");

        assertEquals("tokenMapperMurmur addFields to Document must include a LongField with name and value ",
                longField.numericValue().longValue(), decoratedKey.getToken().getTokenValue());


        Token token =decoratedKey.getToken();

        Query query= tokenMapperMurmur.query(token);
        assertTrue("Query built in TokenMapperGeneric must be NumericRangeQuery",query instanceof
                NumericRangeQuery);

        NumericRangeQuery numericRangeQuery=(NumericRangeQuery)query;

        assertEquals("Min long in NumericRangeQuery must be token value",numericRangeQuery.getMin(), token.getTokenValue());
        assertEquals("Max long in NumericRangeQuery must be token value",numericRangeQuery.getMax(),token.getTokenValue());


        List<SortField> listSort=tokenMapperMurmur.sortFields();
        assertEquals("TokenMapperGeneric.sortFields() must return a 1 elem list",listSort.size(),1);

        SortField sortField=listSort.get(0);

        assertEquals("SortField returned by TokenMapperGeneric.sortFields() must be equal to SortField(FIELD_NAME, "
                + "SortField.Type.LONG)", sortField, new SortField(TokenMapperGeneric.FIELD_NAME, SortField.Type.LONG));

        /* need to cock StorageService.getPartitioner
        Token minToken = new LongToken(Long.MIN_VALUE);
        Token maxToken= new LongToken(Long.MAX_VALUE);

        Token normalToken1= new LongToken(10l);
        Token normalToken2= new LongToken(20l);

        Query usualQuery=tokenMapperMurmur.doQuery(normalToken1,normalToken2,false, false);

        assertNotNull(usualQuery);
        assertTrue("TokenMapperGeneric.doQuery must return a DocValuesRangeQuery", usualQuery instanceof
                DocValuesRangeQuery);




        Query compareQuery=DocValuesRangeQuery.newLongRange(TokenMapperGeneric.FIELD_NAME,(Long)
                normalToken1.getTokenValue(),(Long) normalToken2.getTokenValue(), false, false);


        assertEquals(usualQuery,compareQuery);


        usualQuery=tokenMapperMurmur.doQuery(normalToken1,normalToken2,false, true);

        assertNotNull(usualQuery);
        assertTrue("TokenMapperGeneric.doQuery must return a DocValuesRangeQuery", usualQuery instanceof
                DocValuesRangeQuery);

        compareQuery=DocValuesRangeQuery.newLongRange(TokenMapperGeneric.FIELD_NAME,(Long)
                normalToken1.getTokenValue(),(Long) normalToken2.getTokenValue(), false, true);


        assertEquals(usualQuery,compareQuery);


        usualQuery=tokenMapperMurmur.doQuery(normalToken1,normalToken2,true, false);

        assertNotNull(usualQuery);
        assertTrue("TokenMapperGeneric.doQuery must return a DocValuesRangeQuery", usualQuery instanceof
                DocValuesRangeQuery);

        compareQuery=DocValuesRangeQuery.newLongRange(TokenMapperGeneric.FIELD_NAME,(Long)
                normalToken1.getTokenValue(),(Long) normalToken2.getTokenValue(), true, false);


        assertEquals(usualQuery,compareQuery);

        usualQuery=tokenMapperMurmur.doQuery(normalToken1,normalToken2,true, true);

        assertNotNull(usualQuery);
        assertTrue("TokenMapperGeneric.doQuery must return a DocValuesRangeQuery", usualQuery instanceof
                DocValuesRangeQuery);

        compareQuery=DocValuesRangeQuery.newLongRange(TokenMapperGeneric.FIELD_NAME,(Long)
                normalToken1.getTokenValue(),(Long) normalToken2.getTokenValue(), true, true);


        assertEquals(usualQuery,compareQuery);

        Query minNullQuery=tokenMapperMurmur.doQuery(null,normalToken2,false, false);

        assertNotNull(minNullQuery);
        assertTrue("TokenMapperGeneric.doQuery must return a DocValuesRangeQuery", minNullQuery instanceof
                DocValuesRangeQuery);

        compareQuery=DocValuesRangeQuery.newLongRange(TokenMapperGeneric.FIELD_NAME,null,(Long) normalToken2
                .getTokenValue(), false, false);

        assertEquals(minNullQuery,compareQuery);

        Query maxNullQuery=tokenMapperMurmur.doQuery(normalToken1,null,false, false);

        assertNotNull(maxNullQuery);
        assertTrue("TokenMapperGeneric.doQuery must return a DocValuesRangeQuery", maxNullQuery instanceof
                DocValuesRangeQuery);

        compareQuery=DocValuesRangeQuery.newLongRange(TokenMapperGeneric.FIELD_NAME,(Long) normalToken1
                .getTokenValue(),null, false, false);

        assertEquals(maxNullQuery,compareQuery);



        Query minQuery=tokenMapperMurmur.doQuery(minToken,normalToken2,false, false);

        assertNotNull(minQuery);
        assertTrue("TokenMapperGeneric.doQuery must return a DocValuesRangeQuery", minQuery instanceof
                DocValuesRangeQuery);

        compareQuery=DocValuesRangeQuery.newLongRange(TokenMapperGeneric.FIELD_NAME,null,(Long) normalToken2
                .getTokenValue(), false, false);

        assertEquals(minQuery,compareQuery);


        Query maxQuery=tokenMapperMurmur.doQuery(normalToken1,maxToken,false, false);

        assertNotNull(maxQuery);
        assertTrue("TokenMapperGeneric.doQuery must return a DocValuesRangeQuery", maxQuery instanceof
                DocValuesRangeQuery);

        compareQuery=DocValuesRangeQuery.newLongRange(TokenMapperGeneric.FIELD_NAME,(Long) normalToken1
                .getTokenValue(),null, false, false);

        assertEquals(maxQuery,compareQuery);

        */

    }
}