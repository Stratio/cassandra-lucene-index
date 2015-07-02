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

import com.stratio.cassandra.lucene.schema.mapping.DoubleMapper;
import com.stratio.cassandra.lucene.schema.mapping.LongMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Class for testing {@link LongMapperBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LongMapperBuilderTest extends AbstractMapperBuilderTest {

    @Test
    public void testBuild() {
        LongMapperBuilder builder = new LongMapperBuilder().indexed(false).sorted(false).boost(0.3f);
        LongMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertFalse(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertEquals(0.3f, mapper.getBoost(), 0);
    }

    @Test
    public void testBuildDefaults() {
        LongMapperBuilder builder = new LongMapperBuilder();
        LongMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertEquals(DoubleMapper.DEFAULT_BOOST, mapper.getBoost(), 0);
    }

    @Test
    public void testJsonSerialization() {
        LongMapperBuilder builder = new LongMapperBuilder().indexed(false).sorted(false).boost(0.3f);
        testJsonSerialization(builder, "{type:\"long\",indexed:false,sorted:false,boost:0.3}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        LongMapperBuilder builder = new LongMapperBuilder().indexed(false).sorted(false);
        testJsonSerialization(builder, "{type:\"long\",indexed:false,sorted:false}");
    }
}
