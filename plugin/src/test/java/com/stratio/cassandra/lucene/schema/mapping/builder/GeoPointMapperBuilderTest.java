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

import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Class for testing {@link GeoPointMapperBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoPointMapperBuilderTest extends AbstractMapperBuilderTest {

    @Test
    public void testBuild() {
        GeoPointMapperBuilder builder = new GeoPointMapperBuilder("lat", "lon").maxLevels(5);
        GeoPointMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertEquals("lat", mapper.getLatitude());
        assertEquals("lon", mapper.getLongitude());
        assertEquals(5, mapper.getMaxLevels());
    }

    @Test
    public void testBuildDefaults() {
        GeoPointMapperBuilder builder = new GeoPointMapperBuilder("lat", "lon");
        GeoPointMapper mapper = builder.build("field");
        assertNotNull(mapper);
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("field", mapper.getName());
        assertEquals("lat", mapper.getLatitude());
        assertEquals("lon", mapper.getLongitude());
        assertEquals(GeoPointMapper.DEFAULT_MAX_LEVELS, mapper.getMaxLevels());
    }

    @Test
    public void testJsonSerialization() {
        GeoPointMapperBuilder builder = new GeoPointMapperBuilder("lat", "lon").maxLevels(5);
        testJsonSerialization(builder, "{type:\"geo_point\",latitude:\"lat\",longitude:\"lon\",max_levels:5}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        GeoPointMapperBuilder builder = new GeoPointMapperBuilder("lat", "lon");
        testJsonSerialization(builder, "{type:\"geo_point\",latitude:\"lat\",longitude:\"lon\"}");
    }
}
