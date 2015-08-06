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

package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class AllConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        Float boost = 0.7f;
        AllCondition condition = new AllCondition(boost);
        assertEquals(boost, condition.boost, 0);
    }

    @Test
    public void testBuildWithDefaults() {
        AllCondition condition = new AllCondition(null);
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test
    public void testQuery() {
        AllCondition condition = new AllCondition(0.7f);
        Query query = condition.query(mock(Schema.class));
        assertNotNull(query);
        assertEquals(MatchAllDocsQuery.class, query.getClass());
        assertEquals(0.7f, query.getBoost(), 0);
    }

    @Test
    public void testToString() {
        AllCondition condition = new AllCondition(0.7f);
        assertEquals("AllCondition{boost=0.7}", condition.toString());
    }

}
