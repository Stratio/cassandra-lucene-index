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
import com.stratio.cassandra.lucene.query.RangeCondition;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Class for testing {@link RangeConditionBuilder}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RangeConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuildString() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field").boost(0.4)
                                                                          .lower("lower")
                                                                          .upper("upper")
                                                                          .includeLower(false)
                                                                          .includeUpper(true);
        RangeCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(0.4f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals("lower", condition.lower);
        assertEquals("upper", condition.upper);
        assertEquals(false, condition.includeLower);
        assertEquals(true, condition.includeUpper);
    }

    @Test
    public void testBuildNumber() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field").boost(0.4)
                                                                          .lower(1)
                                                                          .upper(2)
                                                                          .includeLower(false)
                                                                          .includeUpper(true);
        RangeCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(0.4f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals(1, condition.lower);
        assertEquals(2, condition.upper);
        assertEquals(false, condition.includeLower);
        assertEquals(true, condition.includeUpper);
    }

    @Test
    public void testBuildDefaults() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field");
        RangeCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals("field", condition.field);
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
        assertNull(condition.lower);
        assertNull(condition.upper);
        assertEquals(RangeCondition.DEFAULT_INCLUDE_LOWER, condition.includeLower);
        assertEquals(RangeCondition.DEFAULT_INCLUDE_UPPER, condition.includeUpper);
    }

    @Test
    public void testJsonSerializationString() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field").boost(0.4)
                                                                          .lower("lower")
                                                                          .upper("upper")
                                                                          .includeLower(false)
                                                                          .includeUpper(true);
        testJsonSerialization(builder,
                              "{type:\"range\",field:\"field\",boost:0.4,lower:\"lower\",upper:\"upper\"," +
                              "include_lower:false,include_upper:true}");
    }

    @Test
    public void testJsonSerializationNumber() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field").boost(0.4)
                                                                          .lower(1)
                                                                          .upper(2)
                                                                          .includeLower(false)
                                                                          .includeUpper(true);
        testJsonSerialization(builder,
                              "{type:\"range\",field:\"field\",boost:0.4,lower:1,upper:2," +
                              "include_lower:false,include_upper:true}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        PrefixConditionBuilder builder = new PrefixConditionBuilder("field", "value");
        testJsonSerialization(builder, "{type:\"prefix\",field:\"field\",value:\"value\"}");
    }
}
