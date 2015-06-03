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

import com.stratio.cassandra.lucene.schema.mapping.builder.ColumnMapperBuilder;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoPointMapperBuilder extends ColumnMapperBuilder<GeoPointMapper> {

    @JsonProperty("indexed")
    private Boolean indexed;

    @JsonProperty("sorted")
    private Boolean sorted;

    @JsonProperty("latitude")
    private String latitude;

    @JsonProperty("longitude")
    private String longitude;

    @JsonProperty("max_levels")
    private Integer maxLevels;

    public GeoPointMapperBuilder setIndexed(Boolean indexed) {
        this.indexed = indexed;
        return this;
    }

    public GeoPointMapperBuilder setSorted(Boolean sorted) {
        this.sorted = sorted;
        return this;
    }

    public GeoPointMapperBuilder setLatitude(String latitude) {
        this.latitude = latitude;
        return this;
    }

    public GeoPointMapperBuilder setLongitude(String longitude) {
        this.longitude = longitude;
        return this;
    }

    public GeoPointMapperBuilder setMaxLevels(Integer maxLevels) {
        this.maxLevels = maxLevels;
        return this;
    }

    @Override
    public GeoPointMapper build(String name) {
        GeoPointMapper mapper = new GeoPointMapper(name, indexed, sorted, latitude, longitude, maxLevels);
        return mapper;
    }
}
