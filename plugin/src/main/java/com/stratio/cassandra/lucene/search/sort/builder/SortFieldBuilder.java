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

import com.stratio.cassandra.lucene.search.sort.SortField;
import com.stratio.cassandra.lucene.util.Builder;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * {@link Builder} for building a new {@link SortField}.
 *
 * @param <T> the {@link SortField}
 * @param <K> the {@link SortField} builder
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", defaultImpl = SimpleSortFieldBuilder.class)
@JsonSubTypes({@JsonSubTypes.Type(value = SimpleSortFieldBuilder.class, name = "simple"),
               @JsonSubTypes.Type(value = GeoDistanceSortFieldBuilder.class, name = "geo_distance")})
public abstract class SortFieldBuilder<T extends SortField, K extends SortFieldBuilder> implements Builder<T> {

    /** If natural order should be reversed. */
    @JsonProperty("reverse")
    boolean reverse;

    /**
     * Returns this {@link SortFieldBuilder} with the specified reverse option.
     *
     * @param reverse {@code true} if natural order should be reversed
     * @return This.
     */
    @SuppressWarnings("unchecked")
    public K reverse(boolean reverse) {
        this.reverse = reverse;
        return (K) this;
    }

    /** {@inheritDoc} */
    @Override
    public abstract T build();
}