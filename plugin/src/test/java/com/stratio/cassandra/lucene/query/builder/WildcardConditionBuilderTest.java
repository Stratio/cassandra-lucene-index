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
import com.stratio.cassandra.lucene.query.WildcardCondition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Class for testing {@link WildcardConditionBuilder}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class WildcardConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuild() {
        WildcardConditionBuilder builder = new WildcardConditionBuilder("field", "value").boost(0.7f);
        WildcardCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(0.7f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals("value", condition.value);
    }

    @Test
    public void testBuildDefaults() {
        WildcardConditionBuilder builder = new WildcardConditionBuilder("field", "value");
        WildcardCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals("value", condition.value);
    }

    @Test
    public void testJsonSerialization() {
        WildcardConditionBuilder builder = new WildcardConditionBuilder("field", "value").boost(0.7f);
        testJsonSerialization(builder, "{type:\"wildcard\",field:\"field\",value:\"value\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        WildcardConditionBuilder builder = new WildcardConditionBuilder("field", "value");
        testJsonSerialization(builder, "{type:\"wildcard\",field:\"field\",value:\"value\"}");
    }
}
