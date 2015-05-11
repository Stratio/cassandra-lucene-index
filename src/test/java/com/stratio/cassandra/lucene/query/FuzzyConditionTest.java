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
package com.stratio.cassandra.lucene.query;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperBoolean;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.filter;
import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.fuzzy;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FuzzyConditionTest extends AbstractConditionTest {

    @Test
    public void testFuzzyQuery() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperBoolean(null, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        FuzzyCondition fuzzyCondition = new FuzzyCondition(0.5f, "name", "tr", 1, 2, 49, true);
        Query query = fuzzyCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(org.apache.lucene.search.FuzzyQuery.class, query.getClass());
        org.apache.lucene.search.FuzzyQuery luceneQuery = (org.apache.lucene.search.FuzzyQuery) query;
        Assert.assertEquals("name", luceneQuery.getField());
        Assert.assertEquals("tr", luceneQuery.getTerm().text());
        Assert.assertEquals(1, luceneQuery.getMaxEdits());
        Assert.assertEquals(2, luceneQuery.getPrefixLength());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testJson() {
        testJsonCondition(filter(fuzzy("name", "tr").maxEdits(1)
                                                    .maxExpansions(1)
                                                    .prefixLength(40)
                                                    .transpositions(true)
                                                    .boost(0.5f)));
    }

}
