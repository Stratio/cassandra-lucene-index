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

package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

/**
 * An abstract {@link Condition} using a specific {@link Mapper}.
 *
 * Known subclasses are: <ul> <li> {@link BitemporalCondition} <li> {@link DateRangeCondition} <li> {@link
 * GeoDistanceCondition} <li> {@link GeoBBoxCondition} </ul>
 *
 * @param <T> The specific {@link Mapper} type.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleMapperCondition<T extends Mapper> extends SingleFieldCondition {

    /** The type of the {@link Mapper}. */
    protected final Class<? extends T> type;

    /**
     * Constructor using the boost and the name of the mapper.
     *
     * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link #DEFAULT_BOOST} will be
     * used as default
     * @param field the name of the field to be matched
     * @param type the type of the {@link Mapper}
     */
    protected SingleMapperCondition(Float boost, String field, Class<? extends T> type) {
        super(boost, field);
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public Query query(Schema schema) {
        Mapper mapper = schema.getMapper(field);
        if (mapper == null) {
            throw new IndexException("No mapper found for field '%s'", field);
        } else if (!type.isAssignableFrom(mapper.getClass())) {
            throw new IndexException("Field '%s' requires a mapper of type '%s' but found '%s'", field, type, mapper);
        }
        return query((T) mapper, schema.getAnalyzer());
    }

    /**
     * Returns the Lucene {@link Query} representation of this condition.
     *
     * @param mapper The {@link Mapper} to be used.
     * @param analyzer The {@link Schema} {@link Analyzer}.
     * @return The Lucene {@link Query} representation of this condition.
     */
    @SuppressWarnings("UnusedParameters")
    public abstract Query query(T mapper, Analyzer analyzer);
}