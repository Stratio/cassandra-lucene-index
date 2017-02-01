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
package com.stratio.cassandra.lucene.search;

import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.condition.builder.MatchConditionBuilder;
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.stringMapper;
import static com.stratio.cassandra.lucene.search.SearchBuilders.*;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchTest {

    private static final MatchConditionBuilder MATCH = match("f", "v");
    private static final SortFieldBuilder<?, ?> FIELD = field("f");

    @Test
    public void testBuilderEmpty() {
        Search search = search().build();
        assertFalse("Default refresh is not set", search.refresh());
    }

    @Test
    public void testBuilder() {
        assertTrue("Refresh is not set", search().filter(MATCH)
                                                 .query(MATCH)
                                                 .sort(FIELD)
                                                 .refresh(true)
                                                 .build().refresh());
    }

    @Test
    public void testUsesRelevance() {
        assertFalse("Use relevance is wrong", filter(MATCH).build().usesRelevance());
        assertTrue("Use relevance is wrong", query(MATCH).build().usesRelevance());
        assertFalse("Use relevance is wrong", sort(FIELD).build().usesRelevance());
        assertTrue("Use relevance is wrong", filter(MATCH).query(MATCH).sort(FIELD).build().usesRelevance());
        assertTrue("Use relevance is wrong", query(MATCH).sort(FIELD).build().usesRelevance());
        assertFalse("Use relevance is wrong", filter(MATCH).sort(FIELD).build().usesRelevance());
    }

    @Test
    public void testUsesSorting() {
        assertFalse("Use sorting is wrong", filter(MATCH).build().usesSorting());
        assertFalse("Use sorting is wrong", query(MATCH).build().usesSorting());
        assertTrue("Use sorting is wrong", sort(FIELD).build().usesSorting());
        assertTrue("Use sorting is wrong",
                   search().filter(match("field", "v"))
                           .query(match("field", "v"))
                           .sort(field("field"))
                           .build()
                           .usesRelevance());
    }

    @Test
    public void testRequiresFullScan() {
        assertFalse("Requires full scan is wrong", filter(MATCH).build().requiresFullScan());
        assertTrue("Requires full scan is wrong", query(MATCH).build().requiresFullScan());
        assertFalse("Requires full scan is wrong", filter(MATCH).refresh(true).build().requiresFullScan());
        assertTrue("Requires full scan is wrong", sort(FIELD).build().requiresFullScan());
        assertTrue("Requires full scan is wrong", refresh(true).build().requiresFullScan());
        assertFalse("Requires full scan is wrong", refresh(false).build().requiresFullScan());
        assertFalse("Requires full scan is wrong", search().build().requiresFullScan());
        assertTrue("Requires full scan is wrong", search().query(MATCH).sort(FIELD).build().requiresFullScan());
        assertTrue("Requires full scan is wrong",
                   search().filter(MATCH).query(MATCH).sort(FIELD).build().requiresFullScan());
    }

    @Test
    public void testRequiresPostProcessing() {
        assertFalse("Requires post processing is wrong", filter(MATCH).build().requiresPostProcessing());
        assertTrue("Requires post processing is wrong", query(MATCH).build().requiresPostProcessing());
        assertFalse("Requires post processing is wrong", filter(MATCH).refresh(true).build().requiresPostProcessing());
        assertTrue("Requires post processing is wrong", sort(FIELD).build().requiresPostProcessing());
        assertFalse("Requires post processing is wrong", refresh(true).build().requiresPostProcessing());
        assertFalse("Requires post processing is wrong", refresh(false).build().requiresPostProcessing());
        assertFalse("Requires post processing is wrong", search().build().requiresPostProcessing());
        assertTrue("Requires post processing is wrong",
                   search().filter(MATCH).sort(FIELD).build().requiresPostProcessing());
        assertTrue("Requires post processing is wrong",
                   search().filter(MATCH).query(MATCH).sort(FIELD).build().requiresPostProcessing());
    }

    @Test
    public void testSort() {
        Schema schema = schema().mapper("f", stringMapper()).build();
        assertNotNull("Sort fields is wrong", sort(FIELD).build().sortFields(schema));
        assertTrue("Sort fields is wrong", filter(MATCH).build().sortFields(schema).isEmpty());
    }

    @Test
    public void testValidate() {
        Schema schema = schema().mapper("f", stringMapper()).build();
        search().filter(MATCH)
                .query(MATCH)
                .sort(FIELD)
                .build()
                .validate(schema);
    }

    @Test
    public void testEmptyQuery() {
        Query query = search().build().query(schema().build(), null);
        assertTrue("Pure negation is wrong", query instanceof MatchAllDocsQuery);
    }

    @Test
    public void testPostProcessingFields() {
        assertEquals("postProcessingFields is wrong",
                     Sets.newHashSet("f2", "f3.f1", "f3.f2", "f3.f3"),
                     search().filter(match("f1", 1))
                             .query(match("f2", 1))
                             .query(bool().must(match("f3.f1", 1))
                                          .must(match("f3.f2", 1))
                                          .should(match("f3.f3", 1))
                                          .not(match("f3.f4", 1)))
                             .build()
                             .postProcessingFields());
    }

    @Test
    public void testToString() {
        Search search = search().filter(match("f1", "v1").docValues(true), match("f2", "v2"))
                                .query(match("f3", "v3"), match("f4", "v4").boost(0.3))
                                .sort(field("f5").reverse(true))
                                .refresh(true)
                                .build();
        assertEquals("Method #toString is wrong",
                     "Search{filter=[MatchCondition{boost=null, field=f1, value=v1, docValues=true}, " +
                     "MatchCondition{boost=null, field=f2, value=v2, docValues=false}], " +
                     "query=[MatchCondition{boost=null, field=f3, value=v3, docValues=false}, " +
                     "MatchCondition{boost=0.3, field=f4, value=v4, docValues=false}], " +
                     "sort=[SimpleSortField{field=f5, reverse=true}], refresh=true, paging=null}",
                     search.toString());
    }

}