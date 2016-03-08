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

package com.stratio.cassandra.lucene.builder.index.schema.mapping;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @param <T> the type of the mapper to be built
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleColumnMapper<T extends SingleColumnMapper<T>> extends Mapper<T> {

    /** If the field must be indexed. */
    @JsonProperty("indexed")
    protected Boolean indexed;

    /** If the field must be sorted. */
    @JsonProperty("sorted")
    protected Boolean sorted;

    /** The name of the column to be mapped. */
    @JsonProperty("column")
    protected String column;

    /**
     * Sets if the field supports searching.
     *
     * @param indexed if the field supports searching
     * @return this with the specified {@code indexed} option
     */
    @SuppressWarnings("unchecked")
    public final T indexed(Boolean indexed) {
        this.indexed = indexed;
        return (T) this;
    }

    /**
     * Sets if the field supports sorting.
     *
     * @param sorted if the field supports sorting
     * @return this with the specified {@code sorted} option
     */
    @SuppressWarnings("unchecked")
    public final T sorted(Boolean sorted) {
        this.sorted = sorted;
        return (T) this;
    }

    /**
     * Sets the name of the Cassandra column to be mapped.
     *
     * @param column the name of the column to be mapped
     * @return this with the specified column
     */
    @SuppressWarnings("unchecked")
    public final T column(String column) {
        this.column = column;
        return (T) this;
    }
}
