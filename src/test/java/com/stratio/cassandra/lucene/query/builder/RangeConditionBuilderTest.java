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
import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertNotNull(condition);
        Assert.assertEquals("field", condition.getField());
        Assert.assertEquals("lower", condition.getLower());
        Assert.assertEquals("upper", condition.getUpper());
        Assert.assertEquals(false, condition.getIncludeLower());
        Assert.assertEquals(true, condition.getIncludeUpper());
    }

    @Test
    public void testBuildNumber() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field");
        builder.lower(1);
        builder.upper(2);
        builder.includeLower(false);
        builder.includeUpper(true);
        RangeCondition condition = builder.build();
        Assert.assertNotNull(condition);
        Assert.assertEquals("field", condition.getField());
        Assert.assertEquals(1, condition.getLower());
        Assert.assertEquals(2, condition.getUpper());
        Assert.assertEquals(false, condition.getIncludeLower());
        Assert.assertEquals(true, condition.getIncludeUpper());
    }

    @Test
    public void testBuildDefaults() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field");
        RangeCondition condition = builder.build();
        Assert.assertNotNull(condition);
        Assert.assertEquals("field", condition.getField());
        Assert.assertNull(condition.getLower());
        Assert.assertNull(condition.getUpper());
        Assert.assertEquals(RangeCondition.DEFAULT_INCLUDE_LOWER, condition.getIncludeLower());
        Assert.assertEquals(RangeCondition.DEFAULT_INCLUDE_UPPER, condition.getIncludeUpper());
    }
}
