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
package com.stratio.cassandra.lucene.builder.index;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.stratio.cassandra.lucene.builder.JSONBuilder;
import com.stratio.cassandra.lucene.builder.index.Partitioner.*;

/**
 * An index partitioner to split the index in multiple partitions.
 *
 * Index partitioning is useful to speed up some searches to the detriment of others, depending on the implementation.
 * It is also useful to overcome the Lucene's hard limit of 2147483519 documents per index.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", defaultImpl = None.class)
@JsonSubTypes({@JsonSubTypes.Type(value = None.class, name = "none"),
               @JsonSubTypes.Type(value = OnToken.class, name = "token")})
public abstract class Partitioner extends JSONBuilder {

    /**
     * {@link Partitioner} with no action, equivalent to not defining a partitioner.
     */
    public static class None extends Partitioner {
    }

    /**
     * {@link Partitioner} based on the Cassandra's partitioning token.
     *
     * Partitioning on token guarantees a good load balancing between partitions while speeding up partition-directed
     * searches to the detriment of token range searches.
     */
    public static class OnToken extends Partitioner {

        @JsonProperty("partitions")
        public final int partitions;

        public OnToken(int partitions) {
            this.partitions = partitions;
        }
    }
}
