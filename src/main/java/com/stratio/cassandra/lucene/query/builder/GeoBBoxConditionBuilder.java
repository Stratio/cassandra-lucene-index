/*
 * Copyright 2014, Stratio.
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
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.GeoBBoxCondition;

/**
 * {@link ConditionBuilder} for building a new {@link GeoBBoxCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoBBoxConditionBuilder extends ConditionBuilder<GeoBBoxCondition, GeoBBoxConditionBuilder> {

    private String field; // The name of the field to be matched
    private double minLongitude; // The minimum accepted longitude
    private double maxLongitude; // The maximum accepted longitude
    private double minLatitude; // The minimum accepted latitude
    private double maxLatitude; // The maximum accepted latitude

    /**
     * Returns a new {@link GeoBBoxConditionBuilder} with the specified field name and bounding box coordinates.
     *
     * @param field        The name of the field to be matched.
     * @param minLongitude The minimum accepted longitude.
     * @param maxLongitude The maximum accepted longitude.
     * @param minLatitude  The minimum accepted latitude.
     * @param maxLatitude  The maximum accepted latitude.
     */
    public GeoBBoxConditionBuilder(String field,
                                   double minLongitude,
                                   double maxLongitude,
                                   double minLatitude,
                                   double maxLatitude) {
        this.field = field;
        this.minLongitude = minLongitude;
        this.maxLongitude = maxLongitude;
        this.minLatitude = minLatitude;
        this.maxLatitude = maxLatitude;
    }

    /**
     * Returns the {@link GeoBBoxCondition} represented by this builder.
     *
     * @return The {@link GeoBBoxCondition} represented by this builder.
     */
    @Override
    public GeoBBoxCondition build() {
        return new GeoBBoxCondition(boost, field, minLongitude, maxLongitude, minLatitude, maxLatitude);
    }
}
