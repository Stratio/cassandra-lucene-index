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

import com.stratio.cassandra.lucene.schema.mapping.InetMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Class for testing {@link InetMapperBuilder}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class InetMapperBuilderTest extends AbstractMapperBuilderTest {

    @Test
    public void testBuild() {
        InetMapperBuilder builder = new InetMapperBuilder().indexed(false).sorted(false);
        InetMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertFalse(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("field", mapper.getName());
    }

    @Test
    public void testBuildDefaults() {
        InetMapperBuilder builder = new InetMapperBuilder();
        InetMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals("field", mapper.getName());
    }

    @Test
    public void testJsonSerialization() {
        InetMapperBuilder builder = new InetMapperBuilder().indexed(false).sorted(false);
        testJsonSerialization(builder, "{type:\"inet\",indexed:false,sorted:false}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        InetMapperBuilder builder = new InetMapperBuilder();
        testJsonSerialization(builder, "{type:\"inet\"}");
    }
}
