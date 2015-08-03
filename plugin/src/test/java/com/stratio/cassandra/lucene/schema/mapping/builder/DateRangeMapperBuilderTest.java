/*
 * Copyright 2014, Stratio.
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

package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.DateRangeMapper;
import com.stratio.cassandra.lucene.util.DateParser;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Class for testing {@link DateRangeMapperBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeMapperBuilderTest extends AbstractMapperBuilderTest {

    @Test
    public void testBuild() {
        DateRangeMapperBuilder builder = new DateRangeMapperBuilder("start", "stop").pattern("yyyy-MM-dd");
        DateRangeMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertEquals("start", mapper.getStart());
        assertEquals("stop", mapper.getStop());
        assertEquals("yyyy-MM-dd", mapper.getPattern());
    }

    @Test
    public void testBuildDefaults() {
        DateRangeMapperBuilder builder = new DateRangeMapperBuilder("start", "stop");
        DateRangeMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertEquals("start", mapper.getStart());
        assertEquals("stop", mapper.getStop());
        assertEquals(DateParser.DEFAULT_PATTERN, mapper.getPattern());
    }

    @Test
    public void testJsonSerialization() {
        DateRangeMapperBuilder builder = new DateRangeMapperBuilder("start", "stop").pattern("yyyy-MM-dd");
        testJsonSerialization(builder, "{type:\"date_range\",start:\"start\",stop:\"stop\",pattern:\"yyyy-MM-dd\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        DateRangeMapperBuilder builder = new DateRangeMapperBuilder("start", "stop");
        testJsonSerialization(builder, "{type:\"date_range\",start:\"start\",stop:\"stop\"}");
    }
}