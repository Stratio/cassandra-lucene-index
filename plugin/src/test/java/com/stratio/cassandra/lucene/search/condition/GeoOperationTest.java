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

package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.common.GeoOperation;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoOperationTest extends AbstractConditionTest {

    @Test(expected = IndexException.class)
    public void testParseNull() {
        GeoOperation.parse(null);
    }

    @Test(expected = IndexException.class)
    public void testParseEmpty() {
        GeoOperation.parse("");
    }

    @Test(expected = IndexException.class)
    public void testParseBlank() {
        GeoOperation.parse("\t ");
    }

    @Test(expected = IndexException.class)
    public void testInvalid() {
        GeoOperation.parse("invalid_operation");
    }

    @Test
    public void testParseIntersects() {
        GeoOperation distance = GeoOperation.parse("intersects");
        check(distance, SpatialOperation.Intersects);
    }

    @Test
    public void testParseIsWithin() {
        GeoOperation distance = GeoOperation.parse("is_within");
        check(distance, SpatialOperation.IsWithin);
    }

    @Test
    public void testParseContains() {
        GeoOperation distance = GeoOperation.parse("contains");
        check(distance, SpatialOperation.Contains);
    }

    private void check(GeoOperation operation, SpatialOperation spatialOperation) {
        assertEquals("Parsed distance is wrong", spatialOperation, operation.getSpatialOperation());
    }
}
