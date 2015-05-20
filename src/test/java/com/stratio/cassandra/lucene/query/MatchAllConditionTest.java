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
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class MatchAllConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        Float boost = 0.7f;
        MatchAllCondition condition = new MatchAllCondition(boost);
        assertEquals(boost, condition.getBoost(), 0);
    }

    @Test
    public void testBuildWithDefaults() {
        MatchAllCondition condition = new MatchAllCondition(null);
        assertEquals(Condition.DEFAULT_BOOST, condition.getBoost(), 0);
    }

    @Test
    public void testQuery() {
        MatchAllCondition condition = new MatchAllCondition(0.7f);
        Query query = condition.query(mock(Schema.class));
        assertNotNull(query);
        assertEquals(MatchAllDocsQuery.class, query.getClass());
        assertEquals(0.7f, query.getBoost(), 0);
    }

    @Test
    public void testJson() throws IOException {
        String in = "{type:\"match_all\",boost:0.7}";
        MatchAllCondition condition = JsonSerializer.fromString(in, MatchAllCondition.class);
        String out = JsonSerializer.toString(condition);
        assertEquals(in, out);
    }

    @Test
    public void testToString() {
        MatchAllCondition condition = new MatchAllCondition(0.7f);
        assertEquals("MatchAllCondition{boost=0.7}", condition.toString());
    }

}
