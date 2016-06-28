/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.search.sort.builder;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.search.sort.GeoDistanceSortField;
import com.stratio.cassandra.lucene.search.sort.SortField;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Class for testing {@link GeoDistanceSortFieldBuilder}.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortFieldBuilderTest {

    @Test
    public void testBuild() {
        String mapper = "geo_place";
        double latitude = 0.0;
        double longitude = 0.0;

        GeoDistanceSortField field = new GeoDistanceSortFieldBuilder(mapper, latitude, longitude).reverse(true).build();
        assertNotNull("GeoDistanceSortField is not built", field);
        assertEquals("GeoDistanceSortField field name is not set", mapper, field.field);
        assertEquals("GeoDistanceSortField reverse is not set", true, field.reverse);
        assertTrue("GeoDistanceSortField latitude is not set", latitude == field.latitude);
        assertTrue("GeoDistanceSortField longitude is not set", longitude == field.longitude);
    }

    @Test
    public void testBuildDefault() {
        String mapper = "geo_place";
        double latitude = 0.0;
        double longitude = 0.0;

        GeoDistanceSortField field = new GeoDistanceSortFieldBuilder(mapper, latitude, longitude).build();
        assertNotNull("GeoDistanceSortField is not built", field);
        assertEquals("GeoDistanceSortField field name is not set", mapper, field.field);
        assertEquals("GeoDistanceSortField reverse is not properly set", SortField.DEFAULT_REVERSE, field.reverse);
    }

    @Test
    public void testBuildInvalidLat() {
        String field = "field";
        double latitude = 91.0;
        double longitude = 0.0;

        GeoDistanceSortFieldBuilder builder = new GeoDistanceSortFieldBuilder(field, latitude, longitude);
        try {
            builder.build();
        } catch (IndexException e) {
            assertEquals("Creating a GeoDistanceSortFieldBuilder with invalid longitude must throw an IndexException",
                         "latitude must be in range [-90.0, 90.0], but found 91.0",
                         e.getMessage());
        }

    }

    @Test
    public void testBuildInvalidLong() {
        String field = "field";
        double latitude = 0.0;
        double longitude = 200.0;

        GeoDistanceSortFieldBuilder builder = new GeoDistanceSortFieldBuilder(field, latitude, longitude);
        try {
            builder.build();
        } catch (IndexException e) {
            assertEquals("Creating a GeoDistanceSortFieldBuilder with invalid longitude must throw an IndexException",
                         "longitude must be in range [-180.0, 180.0], but found 200.0",
                         e.getMessage());
        }
    }

    @Test
    public void testBuildReverse() {
        String mapper = "geo_place";
        GeoDistanceSortField field = new GeoDistanceSortFieldBuilder(mapper, 0.0, 0.0).reverse(false).build();
        assertNotNull("GeoDistanceSortField is not built", field);
        assertEquals("GeoDistanceSortField field name is not set", mapper, field.field);
        assertEquals("GeoDistanceSortField reverse is not set", false, field.reverse);
    }

    @Test
    public void testJson() throws IOException {
        String json1 = "{type:\"geo_distance\",field:\"geo_place\",latitude:0.0,longitude:0.0,reverse:false}";
        String json2 = JsonSerializer.toString(JsonSerializer.fromString(json1, GeoDistanceSortFieldBuilder.class));
        assertEquals("JSON serialization is wrong", json1, json2);
    }

    @Test
    public void testJsonDefault() throws IOException {
        String json1 = "{type:\"geo_distance\",field:\"geo_place\",latitude:0.0,longitude:0.0,reverse:false}";
        GeoDistanceSortFieldBuilder builder = JsonSerializer.fromString(json1, GeoDistanceSortFieldBuilder.class);
        String json2 = JsonSerializer.toString(builder);
        assertEquals("JSON serialization is wrong", json1, json2);
    }

    @Test
    public void testJsonReverse() throws IOException {
        String json1 = "{type:\"geo_distance\",field:\"geo_place\",latitude:0.0,longitude:0.0,reverse:false}";
        String json2 = JsonSerializer.toString(JsonSerializer.fromString(json1, GeoDistanceSortFieldBuilder.class));
        assertEquals("JSON serialization is wrong", json1, json2);
    }
}