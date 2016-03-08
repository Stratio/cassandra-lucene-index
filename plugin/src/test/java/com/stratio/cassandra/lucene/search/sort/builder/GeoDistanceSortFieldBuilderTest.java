/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

        GeoDistanceSortFieldBuilder builder = new GeoDistanceSortFieldBuilder(mapper,
                                                                              longitude,
                                                                              latitude).reverse(true);
        GeoDistanceSortField sortField = builder.build();
        assertNotNull("GeoDistanceSortField is not built", sortField);
        assertEquals("GeoDistanceSortField mapper name is not set", mapper, sortField.getMapper());
        assertEquals("GeoDistanceSortField reverse is not set", true, sortField.isReverse());
        assertTrue("GeoDistanceSortField latitude is not set", latitude == sortField.getLatitude());
        assertTrue("GeoDistanceSortField longitude is not set", longitude == sortField.getLongitude());
    }

    @Test
    public void testBuildDefault() {
        String mapper = "geo_place";
        double latitude = 0.0;
        double longitude = 0.0;

        GeoDistanceSortFieldBuilder builder = new GeoDistanceSortFieldBuilder(mapper, longitude, latitude);
        GeoDistanceSortField sortField = builder.build();
        assertNotNull("GeoDistanceSortField is not built", sortField);
        assertEquals("GeoDistanceSortField mapper name is not set", mapper, sortField.getMapper());
        assertEquals("GeoDistanceSortField reverse is not set to default",
                     SortField.DEFAULT_REVERSE,
                     sortField.isReverse());
    }

    @Test
    public void testBuildInvalidLat() {
        String field = "field";
        double latitude = 91.0;
        double longitude = 0.0;

        GeoDistanceSortFieldBuilder builder = new GeoDistanceSortFieldBuilder(field, longitude, latitude);
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

        GeoDistanceSortFieldBuilder builder = new GeoDistanceSortFieldBuilder(field, longitude, latitude);
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
        double latitude = 0.0;
        double longitude = 0.0;
        GeoDistanceSortFieldBuilder builder = new GeoDistanceSortFieldBuilder(mapper,
                                                                              longitude,
                                                                              latitude).reverse(false);
        GeoDistanceSortField sortField = builder.build();
        assertNotNull("GeoDistanceSortField is not built", sortField);
        assertEquals("GeoDistanceSortField mapper name is not set", mapper, sortField.getMapper());
        assertEquals("GeoDistanceSortField reverse is not set", false, sortField.isReverse());
    }

    @Test
    public void testJson() throws IOException {
        String json1 = "{type:\"geo_distance\",mapper:\"geo_place\",longitude:0.0,latitude:0.0,reverse:false}";
        GeoDistanceSortFieldBuilder sortFieldBuilder = JsonSerializer.fromString(json1,
                                                                                 GeoDistanceSortFieldBuilder.class);
        String json2 = JsonSerializer.toString(sortFieldBuilder);
        assertEquals("JSON serialization is wrong", json1, json2);
    }

    @Test
    public void testJsonDefault() throws IOException {
        String json1 = "{type:\"geo_distance\",mapper:\"geo_place\",longitude:0.0,latitude:0.0,reverse:false}";
        GeoDistanceSortFieldBuilder sortFieldBuilder = JsonSerializer.fromString(json1,
                                                                                 GeoDistanceSortFieldBuilder.class);
        String json2 = JsonSerializer.toString(sortFieldBuilder);
        assertEquals("JSON serialization is wrong", json1, json2);
    }

    @Test
    public void testJsonReverse() throws IOException {
        String json1 = "{type:\"geo_distance\",mapper:\"geo_place\",longitude:0.0,latitude:0.0,reverse:true}";
        GeoDistanceSortFieldBuilder sortFieldBuilder = JsonSerializer.fromString(json1,
                                                                                 GeoDistanceSortFieldBuilder.class);
        String json2 = JsonSerializer.toString(sortFieldBuilder);
        assertEquals("JSON serialization is wrong", json1, json2);
    }
}