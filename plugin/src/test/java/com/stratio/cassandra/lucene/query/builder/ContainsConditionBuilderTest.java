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
import com.stratio.cassandra.lucene.query.ContainsCondition;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ContainsConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuildDefaults() {
        Object[] values = new Object[]{"a", "b"};
        ContainsConditionBuilder builder = new ContainsConditionBuilder("field", values);
        ContainsCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("field", condition.field);
        assertArrayEquals(values, condition.values);
    }

    @Test
    public void testBuildStrings() {
        Object[] values = new Object[]{"a", "b"};
        ContainsConditionBuilder builder = new ContainsConditionBuilder("field", values).boost(0.7);
        ContainsCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(0.7f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertArrayEquals(values, condition.values);
    }

    @Test
    public void testBuildNumbers() {
        Object[] values = new Object[]{1, 2, -3};
        ContainsConditionBuilder builder = new ContainsConditionBuilder("field", values).boost(0.7);
        ContainsCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(0.7f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertArrayEquals(values, condition.values);
    }

    @Test
    public void testJsonSerializationStrings() {
        ContainsConditionBuilder builder = new ContainsConditionBuilder("field", "a", "b").boost(0.7);
        testJsonSerialization(builder, "{type:\"contains\",field:\"field\",values:[\"a\",\"b\"],boost:0.7}");
    }

    @Test
    public void testJsonSerializationNumbers() {
        ContainsConditionBuilder builder = new ContainsConditionBuilder("field", 1, 2, -3).boost(0.7);
        testJsonSerialization(builder, "{type:\"contains\",field:\"field\",values:[1,2,-3],boost:0.7}");
    }
}
