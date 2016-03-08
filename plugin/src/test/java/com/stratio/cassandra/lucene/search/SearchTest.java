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

package com.stratio.cassandra.lucene.search;

import com.stratio.cassandra.lucene.schema.Schema;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.stringMapper;
import static com.stratio.cassandra.lucene.search.SearchBuilders.*;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchTest {

    @Test
    public void testBuilderEmpty() {
        Search search = new Search(null, null, null, null);
        assertFalse("Default refresh is not set", search.refresh());
    }

    @Test
    public void testBuilder() {
        Search search = search().query(match("field", "value"))
                                .filter(match("field", "value"))
                                .sort(field("field"))
                                .refresh(true)
                                .build();
        assertTrue("Refresh is not set", search.refresh());
    }

    @Test
    public void testUsesRelevance() {
        assertTrue("Use relevance is wrong", search().query(match("field", "value")).build().usesRelevance());
        assertFalse("Use relevance is wrong", search().filter(match("field", "value")).build().usesRelevance());
        assertFalse("Use relevance is wrong", search().sort(field("field")).build().usesRelevance());
        assertTrue("Use relevance is wrong",
                   search().query(match("field", "value"))
                           .filter(match("field", "value"))
                           .sort(field("field"))
                           .build()
                           .usesRelevance());
    }

    @Test
    public void testUsesSorting() {
        assertFalse("Use sorting is wrong", search().query(match("field", "value")).build().usesSorting());
        assertFalse("Use sorting is wrong", search().filter(match("field", "value")).build().usesSorting());
        assertTrue("Use sorting is wrong", search().sort(field("field")).build().usesSorting());
        assertTrue("Use sorting is wrong",
                   search().query(match("field", "value"))
                           .filter(match("field", "value"))
                           .sort(field("field"))
                           .build()
                           .usesRelevance());
    }

    @Test
    public void testRequiresFullScan() {
        assertTrue("Requires full scan is wrong", search().query(match("field", "value")).build().requiresFullScan());
        assertFalse("Requires full scan is wrong", search().filter(match("field", "value")).build().requiresFullScan());
        assertFalse("Requires full scan is wrong",
                    search().filter(match("field", "value")).refresh(true).build().requiresFullScan());
        assertTrue("Requires full scan is wrong", search().sort(field("field")).build().requiresFullScan());
        assertTrue("Requires full scan is wrong", search().refresh(true).build().requiresFullScan());
        assertFalse("Requires full scan is wrong", search().refresh(false).build().requiresFullScan());
        assertFalse("Requires full scan is wrong", search().build().requiresFullScan());
        assertTrue("Requires full scan is wrong",
                   search().query(match("field", "value"))
                           .filter(match("field", "value"))
                           .sort(field("field"))
                           .build()
                           .requiresFullScan());
    }

    @Test
    public void testGetSort() {
        assertNotNull("Sort is wrong", search().sort(field("field")).build().getSort());
        assertNull("Sort is wrong", search().query(match("field", "value")).build().getSort());
    }

    @Test
    public void testSort() {
        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();
        assertNotNull("Sort fields is wrong", search().sort(field("field")).build().sortFields(schema));
        assertNull("Sort fields is wrong", search().query(match("field", "value")).build().sortFields(schema));
    }

    @Test
    public void testValidate() {
        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();
        search().query(match("field", "value"))
                .filter(match("field", "value"))
                .sort(field("field"))
                .build()
                .validate(schema);
    }

    @Test
    public void testToString() {
        Search search = search().query(match("field", "value"))
                                .filter(match("field", "value"))
                                .sort(field("field"))
                                .refresh(true)
                                .build();
        assertEquals("Method #toString is wrong", "Search{query=MatchCondition{boost=1.0, field=field, value=value}, " +
                                                  "filter=MatchCondition{boost=1.0, field=field, value=value}, " +
                                                  "sort=Sort{sortFields=[SimpleSortField{field=field, reverse=false}]}, " +
                                                  "refresh=true}", search.toString());
    }

}