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
package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.geospatial.GeoPointMapperBuilder;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = ColumnMapperBlobBuilder.class, name = "bytes"),
               @JsonSubTypes.Type(value = ColumnMapperBooleanBuilder.class, name = "boolean"),
               @JsonSubTypes.Type(value = ColumnMapperDateBuilder.class, name = "date"),
               @JsonSubTypes.Type(value = ColumnMapperDoubleBuilder.class, name = "double"),
               @JsonSubTypes.Type(value = ColumnMapperFloatBuilder.class, name = "float"),
               @JsonSubTypes.Type(value = ColumnMapperInetBuilder.class, name = "inet"),
               @JsonSubTypes.Type(value = ColumnMapperIntegerBuilder.class, name = "integer"),
               @JsonSubTypes.Type(value = ColumnMapperLongBuilder.class, name = "long"),
               @JsonSubTypes.Type(value = ColumnMapperStringBuilder.class, name = "string"),
               @JsonSubTypes.Type(value = ColumnMapperTextBuilder.class, name = "text"),
               @JsonSubTypes.Type(value = ColumnMapperUUIDBuilder.class, name = "uuid"),
               @JsonSubTypes.Type(value = ColumnMapperBigDecimalBuilder.class, name = "bigdec"),
               @JsonSubTypes.Type(value = ColumnMapperBigIntegerBuilder.class, name = "bigint"),
               @JsonSubTypes.Type(value = GeoPointMapperBuilder.class, name = "geo_shape"),})
public abstract class ColumnMapperBuilder<T extends ColumnMapper> {

    public abstract T build(String name);
}
