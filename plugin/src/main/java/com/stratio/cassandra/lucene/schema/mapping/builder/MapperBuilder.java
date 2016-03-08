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

package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * Abstract builder for creating new {@link Mapper}s.
 *
 * @param <T> the type of the specific {@link Mapper} to be built.
 * @param <K> the type of the specific {@link MapperBuilder} implementing this
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = BlobMapperBuilder.class, name = "bytes"),
               @JsonSubTypes.Type(value = BooleanMapperBuilder.class, name = "boolean"),
               @JsonSubTypes.Type(value = DateMapperBuilder.class, name = "date"),
               @JsonSubTypes.Type(value = DoubleMapperBuilder.class, name = "double"),
               @JsonSubTypes.Type(value = FloatMapperBuilder.class, name = "float"),
               @JsonSubTypes.Type(value = InetMapperBuilder.class, name = "inet"),
               @JsonSubTypes.Type(value = IntegerMapperBuilder.class, name = "integer"),
               @JsonSubTypes.Type(value = LongMapperBuilder.class, name = "long"),
               @JsonSubTypes.Type(value = StringMapperBuilder.class, name = "string"),
               @JsonSubTypes.Type(value = TextMapperBuilder.class, name = "text"),
               @JsonSubTypes.Type(value = UUIDMapperBuilder.class, name = "uuid"),
               @JsonSubTypes.Type(value = BigDecimalMapperBuilder.class, name = "bigdec"),
               @JsonSubTypes.Type(value = BigIntegerMapperBuilder.class, name = "bigint"),
               @JsonSubTypes.Type(value = GeoPointMapperBuilder.class, name = "geo_point"),
               @JsonSubTypes.Type(value = GeoShapeMapperBuilder.class, name = "geo_shape"),
               @JsonSubTypes.Type(value = DateRangeMapperBuilder.class, name = "date_range"),
               @JsonSubTypes.Type(value = BitemporalMapperBuilder.class, name = "bitemporal")})
public abstract class MapperBuilder<T extends Mapper, K extends MapperBuilder<T, K>> {

    /** If the field must be validated. */
    @JsonProperty("validated")
    protected Boolean validated;

    /**
     * Sets if the field must be validated.
     *
     * @param validated if the field must be validated
     * @return this
     */
    @SuppressWarnings("unchecked")
    public final K validated(Boolean validated) {
        this.validated = validated;
        return (K) this;
    }

    /**
     * Returns the {@link Mapper} represented by this.
     *
     * @param field the name of the field to be built
     * @return the {@link Mapper} represented by this
     */
    public abstract T build(String field);
}
