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
import com.stratio.cassandra.lucene.query.FuzzyCondition;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FuzzyConditionBuilderTest {

    @Test
    public void testBuild() {
        FuzzyConditionBuilder builder = new FuzzyConditionBuilder("field", "value");
        builder.boost(0.7f);
        builder.maxEdits(2);
        builder.prefixLength(2);
        builder.maxExpansions(49);
        builder.transpositions(true);
        FuzzyCondition condition = builder.build();
        Assert.assertNotNull(condition);
        Assert.assertEquals(0.7f, condition.getBoost(), 0);
        Assert.assertEquals("field", condition.getField());
        Assert.assertEquals("value", condition.getValue());
        Assert.assertEquals(2, condition.getMaxEdits());
        Assert.assertEquals(2, condition.getPrefixLength());
        Assert.assertEquals(49, condition.getMaxExpansions());
        Assert.assertEquals(true, condition.getTranspositions());
    }

    @Test
    public void testBuildDefaults() {
        FuzzyConditionBuilder builder = new FuzzyConditionBuilder("field", "value");
        FuzzyCondition condition = builder.build();
        Assert.assertNotNull(condition);
        Assert.assertEquals(Condition.DEFAULT_BOOST, condition.getBoost(), 0);
        Assert.assertEquals("field", condition.getField());
        Assert.assertEquals("value", condition.getValue());
        Assert.assertEquals(FuzzyCondition.DEFAULT_MAX_EDITS, condition.getMaxEdits());
        Assert.assertEquals(FuzzyCondition.DEFAULT_PREFIX_LENGTH, condition.getPrefixLength());
        Assert.assertEquals(FuzzyCondition.DEFAULT_MAX_EXPANSIONS, condition.getMaxExpansions());
        Assert.assertEquals(FuzzyCondition.DEFAULT_TRANSPOSITIONS, condition.getTranspositions());
    }
}
