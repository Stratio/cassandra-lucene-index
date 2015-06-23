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

import com.stratio.cassandra.lucene.query.RangeCondition;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RangeConditionBuilderTest {

    @Test
    public void testBuildString() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field");
        builder.lower("lower");
        builder.upper("upper");
        builder.includeLower(false);
        builder.includeUpper(true);
        RangeCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals("field", condition.field);
        assertEquals("lower", condition.lower);
        assertEquals("upper", condition.upper);
        assertEquals(false, condition.includeLower);
        assertEquals(true, condition.includeUpper);
    }

    @Test
    public void testBuildNumber() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field");
        builder.lower(1);
        builder.upper(2);
        builder.includeLower(false);
        builder.includeUpper(true);
        RangeCondition condition = builder.build();
        assertNotNull(condition);
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
        assertNull(condition.lower);
        assertNull(condition.upper);
        assertEquals(RangeCondition.DEFAULT_INCLUDE_LOWER, condition.includeLower);
        assertEquals(RangeCondition.DEFAULT_INCLUDE_UPPER, condition.includeUpper);
    }
}
