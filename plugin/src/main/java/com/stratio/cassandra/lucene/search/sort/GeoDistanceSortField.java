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

import com.google.common.base.Objects;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.SpatialStrategy;

import java.util.Comparator;

import static com.stratio.cassandra.lucene.util.GeospatialUtils.CONTEXT;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortField extends SortField {

    /**
     * The name of mapper to use to calculate distance.
     */
    public final String field;

    /**
     * The longitude of the center point to sort by min distance to it.
     */
    public final double longitude;

    /**
     * The latitude of the center point to sort by min distance to it.
     */
    public final double latitude;

    /**
     * Returns a new {@link SortField}.
     *
     * @param field the name of mapper to use to calculate distance
     * @param reverse {@code true} if natural order should be reversed
     * @param latitude the latitude
     * @param longitude the longitude
     */
    public GeoDistanceSortField(String field, Boolean reverse, double latitude, double longitude) {
        super(reverse);
        if (field == null || StringUtils.isBlank(field)) {
            throw new IndexException("Mapper name required");
        }
        this.field = field;
        this.latitude = GeoPointMapper.checkLatitude("latitude", latitude);
        this.longitude = GeoPointMapper.checkLongitude("longitude", longitude);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public org.apache.lucene.search.SortField sortField(Schema schema) {
        final Mapper mapper = schema.getMapper(field);
        if (mapper == null) {
            throw new IndexException("Field '%s' is not found", field);
        } else if (!(mapper instanceof GeoPointMapper)) {
            throw new IndexException("Field '%s' type is not geo_point", field);
        }
        GeoPointMapper geoPointMapper = (GeoPointMapper) mapper;

        SpatialStrategy strategy = geoPointMapper.distanceStrategy;
        Point pt = CONTEXT.makePoint(longitude, latitude);

        ValueSource valueSource = strategy.makeDistanceValueSource(pt, DistanceUtils.DEG_TO_KM);//the distance (in km)
        return valueSource.getSortField(this.reverse);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<Columns> comparator(Schema schema) {
        final Mapper mapper = schema.getMapper(field);
        return new Comparator<Columns>() {
            public int compare(Columns o1, Columns o2) {
                return GeoDistanceSortField.this.compare((GeoPointMapper) mapper, o1, o2);
            }
        };
    }

    protected int compare(GeoPointMapper mapper, Columns o1, Columns o2) {

        if (o1 == null) {
            return o2 == null ? 0 : 1;
        } else if (o2 == null) {
            return -1;
        }

        Double longO1 = mapper.readLongitude(o1);
        Double latO1 = mapper.readLatitude(o1);

        Double longO2 = mapper.readLongitude(o2);
        Double latO2 = mapper.readLatitude(o2);

        Double base1 = distance(longO1, latO1);
        Double base2 = distance(longO2, latO2);

        return compare(base1, base2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("field", field)
                      .add("reverse", reverse)
                      .add("latitude", latitude)
                      .add("longitude", longitude)
                      .toString();
    }

    /**
     * {@inheritDoc}
     */
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
               longitude == other.longitude &&
               latitude == other.latitude;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (reverse ? 1 : 0);
        result = 31 * result + new Double(latitude).hashCode();
        result = 31 * result + new Double(longitude).hashCode();
        return result;
    }

    private Double distance(Double oLon, Double oLat) {
        if ((oLon == null) || (oLat == null)) {
            return null;
        }
        return DistanceUtils.distHaversineRAD(DistanceUtils.toRadians(latitude),
                                              DistanceUtils.toRadians(longitude),
                                              DistanceUtils.toRadians(oLat),
                                              DistanceUtils.toRadians(oLon));
    }
}
