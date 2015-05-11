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
package com.stratio.cassandra.lucene.geospatial;

import com.google.common.base.Objects;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * Class representing a spatial relationship between two shapes.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
enum GeoOperator {

    BBoxWithin("bbox_within", SpatialOperation.BBoxWithin),
    Contains("contains", SpatialOperation.Contains),
    Intersects("intersects", SpatialOperation.Intersects),
    IsEqualTo("is_equal_to", SpatialOperation.IsEqualTo),
    IsDisjointTo("is_disjoint_to", SpatialOperation.IsDisjointTo),
    IsWithin("is_within", SpatialOperation.IsWithin),
    Overlaps("overlaps", SpatialOperation.Overlaps);

    private String name;
    private SpatialOperation spatialOperation;

    /**
     * Builds a new {@link GeoOperator} identified by the specified {@code String} representing the specified Lucene
     * {@link SpatialOperation}.
     *
     * @param name             A identifying {@code String}.
     * @param spatialOperation A Lucene {@link SpatialOperation}.
     */
    GeoOperator(String name, SpatialOperation spatialOperation) {
        this.name = name;
        this.spatialOperation = spatialOperation;
    }

    /**
     * Returns the identifying name.
     *
     * @return The identifying name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the equivalent Lucene {@link SpatialOperation}.
     *
     * @return The equivalent Lucene {@link SpatialOperation}.
     */
    public SpatialOperation getSpatialOperation() {
        return spatialOperation;
    }

    /**
     * Returns the {@link GeoOperator} identified by the specified {@code String}.
     *
     * @param name The identifying name of the {@link GeoOperator} to be returned.
     * @return The {@link GeoOperator} identified by the specified {@code String}.
     */
    @JsonCreator
    public static GeoOperator create(String name) {
        if (name == null) {
            throw new IllegalArgumentException();
        }
        for (GeoOperator v : values()) {
            if (name.equals(v.getName())) {
                return v;
            }
        }
        throw new IllegalArgumentException();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("name", name).add("spatialOperation", spatialOperation).toString();
    }
}
