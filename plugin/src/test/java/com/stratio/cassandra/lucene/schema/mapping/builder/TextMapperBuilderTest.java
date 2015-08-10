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

import com.stratio.cassandra.lucene.schema.mapping.TextMapper;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Class for testing {@link TextMapperBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TextMapperBuilderTest extends AbstractMapperBuilderTest {

    @Test
    public void testBuild() {
        TextMapperBuilder builder = new TextMapperBuilder().indexed(false).sorted(false).analyzer("spanish");
        TextMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertEquals("field", mapper.getName());
        assertFalse(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("spanish", mapper.getAnalyzer());
    }

    @Test
    public void testBuildDefaults() {
        TextMapperBuilder builder = new TextMapperBuilder();
        TextMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertEquals("field", mapper.getName());
        assertEquals(TextMapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(TextMapper.DEFAULT_SORTED, mapper.isSorted());
        assertNull(mapper.getAnalyzer());
    }

    @Test
    public void testJsonSerialization() {
        TextMapperBuilder builder = new TextMapperBuilder().indexed(false).sorted(false).analyzer("spanish");
        testJsonSerialization(builder, "{type:\"text\",indexed:false,sorted:false,analyzer:\"spanish\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        TextMapperBuilder builder = new TextMapperBuilder();
        testJsonSerialization(builder, "{type:\"text\"}");
    }
}
