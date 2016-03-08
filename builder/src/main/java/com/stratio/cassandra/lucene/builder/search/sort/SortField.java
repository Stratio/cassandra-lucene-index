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

package com.stratio.cassandra.lucene.builder.search.sort;

import com.stratio.cassandra.lucene.builder.Builder;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * A sorting for a field of a search.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", defaultImpl = SimpleSortField.class)
@JsonSubTypes({@JsonSubTypes.Type(value = SimpleSortField.class, name = "simple"),
               @JsonSubTypes.Type(value = GeoDistanceSortField.class, name = "geo_distance")})
public abstract class SortField extends Builder {

    /** If natural order should be reversed. */
    @JsonProperty("reverse")
    Boolean reverse;

    /**
     * Returns this {@link SortField} with the specified reverse option.
     *
     * @param reverse {@code true} if natural order should be reversed, {@code false} otherwise
     * @return this with the specified reverse option set
     */
    public SortField reverse(Boolean reverse) {
        this.reverse = reverse;
        return this;
    }
}