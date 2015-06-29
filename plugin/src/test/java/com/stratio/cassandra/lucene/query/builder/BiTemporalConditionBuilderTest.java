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

import com.stratio.cassandra.lucene.query.BiTemporalCondition;
import org.junit.Test;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.matchAll;
import static org.junit.Assert.*;

/**
 * @author Eduardo Alonso <eduardoalonso@stratio.com>
 */
public class BiTemporalConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuildLong() {
        BiTemporalConditionBuilder builder= new BiTemporalConditionBuilder("field");
        builder.setTt_from((long) 1);
        builder.setTt_to((long) 2);
        builder.setVt_from((long) 3);
        builder.setVt_to((long) 4);
        BiTemporalCondition condition=builder.build();
        assertNotNull(condition);
        assertEquals("field",condition.getField());
        assertEquals((long) 1, condition.getTt_from());
        assertEquals((long) 2, condition.getTt_to());
        assertEquals((long) 3, condition.getVt_from());
        assertEquals((long) 4, condition.getVt_to());
    }

    @Test
    public void testBuildString() {
        BiTemporalConditionBuilder builder= new BiTemporalConditionBuilder("field");
        builder.setTt_from("2015/03/20 11:45:32.333");
        builder.setTt_to("2013/03/20 11:45:32.333");
        builder.setVt_from("2012/03/20 11:45:32.333");
        builder.setVt_to("2011/03/20 11:45:32.333");
        BiTemporalCondition condition=builder.build();
        assertNotNull(condition);
        assertEquals("field",condition.getField());
        assertEquals("2015/03/20 11:45:32.333", condition.getTt_from());
        assertEquals("2013/03/20 11:45:32.333", condition.getTt_to());
        assertEquals("2012/03/20 11:45:32.333", condition.getVt_from());
        assertEquals("2011/03/20 11:45:32.333", condition.getVt_to());
    }

    @Test
    public void testBuildDefaults() {
        BiTemporalConditionBuilder builder= new BiTemporalConditionBuilder("field");
        BiTemporalCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals("field", condition.getField());
        assertNull(condition.getTt_from());
        assertNull(condition.getTt_to());
        assertNull(condition.getVt_from());
        assertNull(condition.getVt_to());
    }

    @Test
    public void testJsonSerialization() {
        BiTemporalConditionBuilder builder = new BiTemporalConditionBuilder("field").boost(0.7f);
        testJsonSerialization(builder, "{type:\"bitemporal\",field:\"field\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        BiTemporalConditionBuilder builder = new BiTemporalConditionBuilder("field");
        testJsonSerialization(builder, "{type:\"bitemporal\",field:\"field\"}");
    }
}
