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
package com.stratio.cassandra.lucene.search.sort;

import com.google.common.base.MoreObjects;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.util.GeospatialUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.SpatialStrategy;

import java.util.Collections;
import java.util.Set;

import static com.stratio.cassandra.lucene.util.GeospatialUtils.CONTEXT;

/**
 * {@link SortField} to sort geo points by their distance to a fixed reference point.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortField extends SortField {

    /** The name of mapper to use to calculate distance. */
    public final String field;

    /** The latitude of the center point to sort by min distance to it. */
    public final double latitude;

    /** The longitude of the center point to sort by min distance to it. */
    public final double longitude;

    /**
     * Returns a new {@link SortField}.
     *  @param field the name of the geo point field mapper to use to calculate distance
     * @param reverse {@code true} if natural order should be reversed
     * @param latitude the latitude
     * @param longitude the longitude
     */
    public GeoDistanceSortField(String field, Boolean reverse, double latitude, double longitude) {
        super(reverse);
        if (field == null || StringUtils.isBlank(field)) {
            throw new IndexException("Field name required");
        }
        this.field = field;
        this.latitude = GeospatialUtils.checkLatitude("latitude", latitude);
        this.longitude = GeospatialUtils.checkLongitude("longitude", longitude);
    }

    /** {@inheritDoc} */
    @Override
    public org.apache.lucene.search.SortField sortField(Schema schema) {
        final Mapper mapper = schema.getMapper(field);
        if (mapper == null) {
            throw new IndexException("Field '{}' is not found", field);
        } else if (!(mapper instanceof GeoPointMapper)) {
            throw new IndexException("Field '{}' type is not geo_point", field);
        }
        GeoPointMapper geoPointMapper = (GeoPointMapper) mapper;

        SpatialStrategy strategy = geoPointMapper.distanceStrategy;
        Point pt = CONTEXT.makePoint(longitude, latitude);

        // The distance (in km)
        ValueSource valueSource = strategy.makeDistanceValueSource(pt, DistanceUtils.DEG_TO_KM);
        return valueSource.getSortField(this.reverse);
    }

    /** {@inheritDoc} */
    public Set<String> postProcessingFields() {
        return Collections.singleton(field);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("field", field)
                          .add("reverse", reverse)
                          .add("latitude", latitude)
                          .add("longitude", longitude)
                          .toString();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeoDistanceSortField other = (GeoDistanceSortField) o;
        return reverse == other.reverse &&
               field.equals(other.field) &&
               latitude == other.latitude &&
               longitude == other.longitude;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (reverse ? 1 : 0);
        result = 31 * result + new Double(latitude).hashCode();
        result = 31 * result + new Double(longitude).hashCode();
        return result;
    }
}