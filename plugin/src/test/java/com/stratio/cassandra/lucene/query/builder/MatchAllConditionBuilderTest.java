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
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.Condition;
import com.stratio.cassandra.lucene.query.MatchAllCondition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Class for testing {@link MatchAllConditionBuilder}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class MatchAllConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuild() {
        MatchAllConditionBuilder builder = new MatchAllConditionBuilder().boost(0.7);
        MatchAllCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(0.7f, condition.boost, 0);
    }

    @Test
    public void testBuildDefaults() {
        MatchAllConditionBuilder builder = new MatchAllConditionBuilder();
        MatchAllCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test
    public void testJsonSerialization() {
        MatchAllConditionBuilder builder = new MatchAllConditionBuilder().boost(0.7);
        testJsonSerialization(builder, "{type:\"match_all\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        MatchAllConditionBuilder builder = new MatchAllConditionBuilder();
        testJsonSerialization(builder, "{type:\"match_all\"}");
    }
}
