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

import com.stratio.cassandra.lucene.schema.mapping.BlobMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Class for testing {@link BlobMapperBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BlobMapperBuilderTest extends AbstractMapperBuilderTest {

    @Test
    public void testBuild() {
        BlobMapperBuilder builder = new BlobMapperBuilder().indexed(false).sorted(false);
        BlobMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertFalse(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("field", mapper.getName());
    }

    @Test
    public void testBuildDefaults() {
        BlobMapperBuilder builder = new BlobMapperBuilder();
        BlobMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals("field", mapper.getName());
    }

    @Test
    public void testJsonSerialization() {
        BlobMapperBuilder builder = new BlobMapperBuilder().indexed(false).sorted(false);
        testJsonSerialization(builder, "{type:\"bytes\",indexed:false,sorted:false}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        BlobMapperBuilder builder = new BlobMapperBuilder();
        testJsonSerialization(builder, "{type:\"bytes\"}");
    }
}
