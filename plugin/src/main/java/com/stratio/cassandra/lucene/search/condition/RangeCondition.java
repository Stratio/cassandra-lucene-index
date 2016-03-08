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
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;

/**
 * A {@link Condition} implementation that matches a field within an range of values.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RangeCondition extends SingleColumnCondition {

    /** The default include lower option. */
    public static final boolean DEFAULT_INCLUDE_LOWER = false;

    /** The default include upper option. */
    public static final boolean DEFAULT_INCLUDE_UPPER = false;

    /** The lower accepted value. Maybe null meaning no lower limit. */
    public final Object lower;

    /** The upper accepted value. Maybe null meaning no upper limit. */
    public final Object upper;

    /** If the lower value must be included if not null. */
    public final boolean includeLower;

    /** If the upper value must be included if not null. */
    public final boolean includeUpper;

    /**
     * Constructs a query selecting all fields greater/equal than {@code lowerValue} but less/equal than {@code
     * upperValue}.
     *
     * If an endpoint is null, it is said to be "open". Either or both endpoints may be open. Open endpoints may not be
     * exclusive (you can't select all but the first or last term without explicitly specifying the term to exclude.)
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link #DEFAULT_BOOST} is used as
     * default.
     * @param field the name of the field to be matched.
     * @param lowerValue the lower accepted value. Maybe {@code null} meaning no lower limit
     * @param upperValue the upper accepted value. Maybe {@code null} meaning no upper limit
     * @param includeLower if {@code true}, the {@code lowerValue} is included in the range
     * @param includeUpper if {@code true}, the {@code upperValue} is included in the range
     */
    public RangeCondition(Float boost,
                          String field,
                          Object lowerValue,
                          Object upperValue,
                          Boolean includeLower,
                          Boolean includeUpper) {
        super(boost, field);
        this.lower = lowerValue;
        this.upper = upperValue;
        this.includeLower = includeLower == null ? DEFAULT_INCLUDE_LOWER : includeLower;
        this.includeUpper = includeUpper == null ? DEFAULT_INCLUDE_UPPER : includeUpper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query query(SingleColumnMapper<?> mapper, Analyzer analyzer) {
        Class<?> clazz = mapper.base;
        Query query;
        if (clazz == String.class) {
            String start = (String) mapper.base(field, lower);
            String stop = (String) mapper.base(field, upper);
            query = TermRangeQuery.newStringRange(field, start, stop, includeLower, includeUpper);
        } else if (clazz == Integer.class) {
            Integer start = (Integer) mapper.base(field, lower);
            Integer stop = (Integer) mapper.base(field, upper);
            query = NumericRangeQuery.newIntRange(field, start, stop, includeLower, includeUpper);
        } else if (clazz == Long.class) {
            Long start = (Long) mapper.base(field, lower);
            Long stop = (Long) mapper.base(field, upper);
            query = NumericRangeQuery.newLongRange(field, start, stop, includeLower, includeUpper);
        } else if (clazz == Float.class) {
            Float start = (Float) mapper.base(field, lower);
            Float stop = (Float) mapper.base(field, upper);
            query = NumericRangeQuery.newFloatRange(field, start, stop, includeLower, includeUpper);
        } else if (clazz == Double.class) {
            Double start = (Double) mapper.base(field, lower);
            Double stop = (Double) mapper.base(field, upper);
            query = NumericRangeQuery.newDoubleRange(field, start, stop, includeLower, includeUpper);
        } else {
            throw new IndexException("Range queries are not supported by mapper '%s'", mapper);
        }
        query.setBoost(boost);
        return query;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toStringHelper(this).add("lower", lower)
                                   .add("upper", upper)
                                   .add("includeLower", includeLower)
                                   .add("includeUpper", includeUpper)
                                   .toString();
    }
}