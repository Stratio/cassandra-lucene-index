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
import com.stratio.cassandra.lucene.search.condition.PhraseCondition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Class for testing {@link PhraseConditionBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PhraseConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuild() {
        PhraseConditionBuilder builder = new PhraseConditionBuilder("field", "value1 value2").slop(2).boost(0.7f);
        PhraseCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(0.7f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals("value1 value2", condition.value);
        assertEquals(2, condition.slop);
    }

    @Test
    public void testBuildDefaults() {
        PhraseConditionBuilder builder = new PhraseConditionBuilder("field", "value1 value2");
        PhraseCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals("value1 value2", condition.value);
        assertEquals(PhraseCondition.DEFAULT_SLOP, condition.slop);
    }

    @Test
    public void testJsonSerialization() {
        PhraseConditionBuilder builder = new PhraseConditionBuilder("field", "value1 value2").slop(2).boost(0.7f);
        testJsonSerialization(builder, "{type:\"phrase\",field:\"field\",value:\"value1 value2\",boost:0.7,slop:2}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        PhraseConditionBuilder builder = new PhraseConditionBuilder("field", "value1 value2");
        testJsonSerialization(builder, "{type:\"phrase\",field:\"field\",value:\"value1 value2\"}");
    }
}
