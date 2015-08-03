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

package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.schema.mapping.StringMapper;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Class for testing {@link StringMapperBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class StringMapperBuilderTest extends AbstractMapperBuilderTest {

    @Test
    public void testBuild() {
        StringMapperBuilder builder = new StringMapperBuilder().indexed(false).sorted(false).caseSensitive(false);
        StringMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertFalse(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertFalse(mapper.isCaseSensitive());
    }

    @Test
    public void testBuildDefaults() {
        StringMapperBuilder builder = new StringMapperBuilder();
        StringMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isCaseSensitive());
    }

    @Test
    public void testJsonSerialization() {
        StringMapperBuilder builder = new StringMapperBuilder().indexed(false).sorted(false).caseSensitive(false);
        testJsonSerialization(builder, "{type:\"string\",indexed:false,sorted:false,case_sensitive:false}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        StringMapperBuilder builder = new StringMapperBuilder();
        testJsonSerialization(builder, "{type:\"string\"}");
    }
}
