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

import com.stratio.cassandra.lucene.schema.mapping.BigDecimalMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Class for testing {@link BigDecimalMapperBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigDecimalMapperBuilderTest extends AbstractMapperBuilderTest {

    @Test
    public void testBuild() {
        BigDecimalMapperBuilder builder = new BigDecimalMapperBuilder().indexed(false)
                                                                       .sorted(false)
                                                                       .integerDigits(6)
                                                                       .decimalDigits(8);
        BigDecimalMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertFalse(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertEquals(6, mapper.getIntegerDigits());
        assertEquals(8, mapper.getDecimalDigits());
    }

    @Test
    public void testBuildDefaults() {
        BigDecimalMapperBuilder builder = new BigDecimalMapperBuilder();
        BigDecimalMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertEquals(BigDecimalMapper.DEFAULT_INTEGER_DIGITS, mapper.getIntegerDigits());
        assertEquals(BigDecimalMapper.DEFAULT_DECIMAL_DIGITS, mapper.getDecimalDigits());
    }

    @Test
    public void testJsonSerialization() {
        BigDecimalMapperBuilder builder = new BigDecimalMapperBuilder().indexed(false)
                                                                       .sorted(false)
                                                                       .integerDigits(6)
                                                                       .decimalDigits(8);
        testJsonSerialization(builder,
                              "{type:\"bigdec\",indexed:false,sorted:false,integer_digits:6,decimal_digits:8}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        BigDecimalMapperBuilder builder = new BigDecimalMapperBuilder();
        testJsonSerialization(builder, "{type:\"bigdec\"}");
    }
}
