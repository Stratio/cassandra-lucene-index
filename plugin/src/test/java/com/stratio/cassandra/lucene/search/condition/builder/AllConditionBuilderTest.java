/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.search.condition.AllCondition;
import com.stratio.cassandra.lucene.search.condition.Condition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Class for testing {@link AllConditionBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class AllConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuild() {
        AllConditionBuilder builder = new AllConditionBuilder().boost(0.7);
        AllCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not properly set ", 0.7f, condition.boost, 0);
    }

    @Test
    public void testBuildDefaults() {
        AllConditionBuilder builder = new AllConditionBuilder();
        AllCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Default boost is not properly set ", Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test
    public void testJsonSerialization() {
        AllConditionBuilder builder = new AllConditionBuilder().boost(0.7);
        testJsonSerialization(builder, "{type:\"all\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        AllConditionBuilder builder = new AllConditionBuilder();
        testJsonSerialization(builder, "{type:\"all\"}");
    }
}
