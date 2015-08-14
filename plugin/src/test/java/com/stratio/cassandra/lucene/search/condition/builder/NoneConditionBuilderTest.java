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

package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.search.condition.Condition;
import com.stratio.cassandra.lucene.search.condition.NoneCondition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Class for testing {@link NoneConditionBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class NoneConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuild() {
        NoneConditionBuilder builder = new NoneConditionBuilder().boost(0.7);
        NoneCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
    }

    @Test
    public void testBuildDefaults() {
        NoneConditionBuilder builder = new NoneConditionBuilder();
        NoneCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set to default value", Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test
    public void testJsonSerialization() {
        NoneConditionBuilder builder = new NoneConditionBuilder().boost(0.7);
        testJsonSerialization(builder, "{type:\"none\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        NoneConditionBuilder builder = new NoneConditionBuilder();
        testJsonSerialization(builder, "{type:\"none\"}");
    }
}
