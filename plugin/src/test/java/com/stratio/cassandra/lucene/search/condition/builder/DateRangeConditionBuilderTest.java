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
import com.stratio.cassandra.lucene.search.condition.DateRangeCondition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Class for testing {@link DateRangeConditionBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuildString() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field").boost(0.4)
                                                                                  .from("2015/01/05")
                                                                                  .to("2015/01/08")
                                                                                  .operation("intersects");
        DateRangeCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(0.4f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals("2015/01/05", condition.from);
        assertEquals("2015/01/08", condition.to);
        assertEquals("intersects", condition.operation);
    }

    @Test
    public void testBuildNumber() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field").boost(0.4)
                                                                                  .from(1)
                                                                                  .to(2)
                                                                                  .operation("is_within");
        DateRangeCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(0.4f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals(1, condition.from);
        assertEquals(2, condition.to);
        assertEquals("is_within", condition.operation);
    }

    @Test
    public void testBuildDefaults() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field");
        DateRangeCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals("field", condition.field);
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals(DateRangeCondition.DEFAULT_FROM, condition.from);
        assertEquals(DateRangeCondition.DEFAULT_TO, condition.to);
        assertEquals(DateRangeCondition.DEFAULT_OPERATION, condition.operation);
    }

    @Test
    public void testJsonSerializationString() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field").boost(0.4)
                                                                                  .from("from")
                                                                                  .to("to")
                                                                                  .operation("intersects");
        testJsonSerialization(builder,
                              "{type:\"date_range\",field:\"field\",boost:0.4," +
                              "from:\"from\",to:\"to\",operation:\"intersects\"}");
    }

    @Test
    public void testJsonSerializationNumber() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field").boost(0.4)
                                                                                  .from("2015/01/05")
                                                                                  .to("2015/01/08")
                                                                                  .operation("contains");
        testJsonSerialization(builder,
                              "{type:\"date_range\",field:\"field\",boost:0.4," +
                              "from:\"2015/01/05\",to:\"2015/01/08\",operation:\"contains\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field");
        testJsonSerialization(builder, "{type:\"date_range\",field:\"field\"}");
    }
}
