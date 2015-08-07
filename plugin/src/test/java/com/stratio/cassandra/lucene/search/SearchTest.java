/*
 * Copyright 2015, Stratio.
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

package com.stratio.cassandra.lucene.search;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.schema.mapping.StringMapper;
import org.junit.Test;

import static com.stratio.cassandra.lucene.search.SearchBuilders.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchTest {

    @Test
    public void testBuilderEmpty() {
        Search search = new Search(null, null, null, null);
        assertFalse(search.refresh());
        assertNotNull(search);
    }

    @Test
    public void testBuilder() {
        Search search = search().query(match("field", "value"))
                                .filter(match("field", "value"))
                                .sort(sortField("field"))
                                .refresh(true)
                                .build();
        assertNotNull(search);
        assertTrue(search.refresh());
    }

    @Test
    public void testUsesRelevance() {
        assertTrue(search().query(match("field", "value")).build().usesRelevance());
        assertFalse(search().filter(match("field", "value")).build().usesRelevance());
        assertFalse(search().sort(sortField("field")).build().usesRelevance());
        assertTrue(search().query(match("field", "value"))
                           .filter(match("field", "value"))
                           .sort(sortField("field"))
                           .build()
                           .usesRelevance());
    }

    @Test
    public void testUsesSorting() {
        assertFalse(search().query(match("field", "value")).build().usesSorting());
        assertFalse(search().filter(match("field", "value")).build().usesSorting());
        assertTrue(search().sort(sortField("field")).build().usesSorting());
        assertTrue(search().query(match("field", "value"))
                           .filter(match("field", "value"))
                           .sort(sortField("field"))
                           .build()
                           .usesRelevance());
    }

    @Test
    public void testRequiresFullScan() {
        assertTrue(search().query(match("field", "value")).build().requiresFullScan());
        assertFalse(search().filter(match("field", "value")).build().requiresFullScan());
        assertFalse(search().filter(match("field", "value")).refresh(true).build().requiresFullScan());
        assertTrue(search().sort(sortField("field")).build().requiresFullScan());
        assertTrue(search().refresh(true).build().requiresFullScan());
        assertFalse(search().refresh(false).build().requiresFullScan());
        assertFalse(search().build().requiresFullScan());
        assertTrue(search().query(match("field", "value"))
                           .filter(match("field", "value"))
                           .sort(sortField("field"))
                           .build()
                           .requiresFullScan());
    }

    @Test
    public void testGetSort() {
        assertNotNull(search().sort(sortField("field")).build().getSort());
        assertNull(search().query(match("field", "value")).build().getSort());
    }

    @Test
    public void testSort() {
        Mapper mapper = new StringMapper("field", true, true, true);
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.STANDARD.get());
        when(schema.getMapper("field")).thenReturn(mapper);
        assertNotNull(search().sort(sortField("field")).build().sortFields(schema));
        assertNull(search().query(match("field", "value")).build().sortFields(schema));
    }

    @Test
    public void testValidate() {
        Mapper mapper = new StringMapper("field", true, true, true);
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.STANDARD.get());
        when(schema.getMapper("field")).thenReturn(mapper);
        search().query(match("field", "value"))
                .filter(match("field", "value"))
                .sort(sortField("field"))
                .build()
                .validate(schema);
    }

    @Test
    public void testToString() {
        Search search = search().query(match("field", "value"))
                                .filter(match("field", "value"))
                                .sort(sortField("field"))
                                .build();
        assertEquals("Search{queryCondition=MatchCondition{boost=1.0, field=field, value=value}, " +
                     "filterCondition=MatchCondition{boost=1.0, field=field, value=value}, " +
                     "sort=Sort{sortFields=[SortField{field=field, reverse=false}]}}", search.toString());
    }

}
