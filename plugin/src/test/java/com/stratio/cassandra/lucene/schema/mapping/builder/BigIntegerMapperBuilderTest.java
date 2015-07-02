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

import com.stratio.cassandra.lucene.schema.mapping.BigIntegerMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Class for testing {@link BigIntegerMapperBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigIntegerMapperBuilderTest extends AbstractMapperBuilderTest {

    @Test
    public void testBuild() {
        BigIntegerMapperBuilder builder = new BigIntegerMapperBuilder().indexed(false).sorted(false).digits(6);
        BigIntegerMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertFalse(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertEquals(6, mapper.getDigits());
    }

    @Test
    public void testBuildDefaults() {
        BigIntegerMapperBuilder builder = new BigIntegerMapperBuilder();
        BigIntegerMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertEquals(BigIntegerMapper.DEFAULT_DIGITS, mapper.getDigits());
    }

    @Test
    public void testJsonSerialization() {
        BigIntegerMapperBuilder builder = new BigIntegerMapperBuilder().indexed(false).sorted(false).digits(6);
        testJsonSerialization(builder, "{type:\"bigint\",indexed:false,sorted:false,digits:6}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        BigIntegerMapperBuilder builder = new BigIntegerMapperBuilder();
        testJsonSerialization(builder, "{type:\"bigint\"}");
    }
}
