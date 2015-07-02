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
import com.stratio.cassandra.lucene.search.condition.LuceneCondition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Class for testing {@link LuceneConditionBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LuceneConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuild() {
        LuceneConditionBuilder builder = new LuceneConditionBuilder("field:value").boost(0.7f).defaultField("field");
        LuceneCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(0.7f, condition.boost, 0);
        assertEquals("field", condition.defaultField);
        assertEquals("field:value", condition.query);
    }

    @Test
    public void testBuildDefaults() {
        LuceneConditionBuilder builder = new LuceneConditionBuilder("field:value");
        LuceneCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals(LuceneCondition.DEFAULT_FIELD, condition.defaultField);
        assertEquals("field:value", condition.query);
    }

    @Test
    public void testJsonSerialization() {
        LuceneConditionBuilder builder = new LuceneConditionBuilder("field:value").boost(0.7f).defaultField("field");
        testJsonSerialization(builder, "{type:\"lucene\",query:\"field:value\",boost:0.7,default_field:\"field\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        LuceneConditionBuilder builder = new LuceneConditionBuilder("field:value");
        testJsonSerialization(builder, "{type:\"lucene\",query:\"field:value\"}");
    }
}
