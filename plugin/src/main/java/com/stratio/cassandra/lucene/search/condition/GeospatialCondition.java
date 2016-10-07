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
package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.GeoShapeMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;

/**
 * {@link Condition} for geospatial searches.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class GeospatialCondition extends SingleFieldCondition {

    private String queryTypeName;

    /**
     * Abstract {@link GeospatialCondition} builder receiving the boost to be used.
     *
     * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     * @param queryTypeName the name of the type of query to be shown on user-directed messages
     */
    public GeospatialCondition(Float boost, String field, String queryTypeName) {
        super(boost, field);
        this.queryTypeName = queryTypeName;
    }

    /** {@inheritDoc} */
    @Override
    public final Query doQuery(Schema schema) {

        // Get the spatial strategy from the mapper
        SpatialStrategy strategy;
        Mapper mapper = schema.mapper(field);
        if (mapper == null) {
            throw new IndexException("No mapper found for field '{}'", field);
        } else if (mapper instanceof GeoShapeMapper) {
            strategy = ((GeoShapeMapper) mapper).strategy;
        } else if (mapper instanceof GeoPointMapper) {
            strategy = ((GeoPointMapper) mapper).strategy;
        } else {
            throw new IndexException("'{}' search requires a 'geo_point' or 'geo_shape' mapper but found {}:{}",
                                     queryTypeName, field, mapper);
        }

        // Delegate query creation
        return doQuery(strategy);
    }

    /**
     * Returns the Lucene {@link Query} representation of this condition without boost.
     *
     * @param strategy the geospatial strategy defined by the mapper
     * @return a Lucene query
     */
    public abstract Query doQuery(SpatialStrategy strategy);
}
