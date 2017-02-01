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
package com.stratio.cassandra.lucene.search.condition;

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.DocValuesRangeQuery;
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

    /** The default use doc values option. */
    public static final boolean DEFAULT_DOC_VALUES = false;

    /** The lower accepted value. Maybe null meaning no lower limit. */
    public final Object lower;

    /** The upper accepted value. Maybe null meaning no upper limit. */
    public final Object upper;

    /** If the lower value must be included if not null. */
    public final boolean includeLower;

    /** If the upper value must be included if not null. */
    public final boolean includeUpper;

    /** If the generated query should use doc values. */
    public final boolean docValues;

    /**
     * Constructs a query selecting all fields greater/equal than {@code lowerValue} but less/equal than {@code
     * upperValue}.
     *
     * If an endpoint is null, it is said to be "open". Either or both endpoints may be open. Open endpoints may not be
     * exclusive (you can't select all but the first or last term without explicitly specifying the term to exclude.)
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched.
     * @param lowerValue the lower accepted value. Maybe {@code null} meaning no lower limit
     * @param upperValue the upper accepted value. Maybe {@code null} meaning no upper limit
     * @param includeLower if {@code true}, the {@code lowerValue} is included in the range
     * @param includeUpper if {@code true}, the {@code upperValue} is included in the range
     * @param docValues if the generated query should use doc values
     */
    public RangeCondition(Float boost,
                          String field,
                          Object lowerValue,
                          Object upperValue,
                          Boolean includeLower,
                          Boolean includeUpper,
                          Boolean docValues) {
        super(boost, field);
        this.lower = lowerValue;
        this.upper = upperValue;
        this.includeLower = includeLower == null ? DEFAULT_INCLUDE_LOWER : includeLower;
        this.includeUpper = includeUpper == null ? DEFAULT_INCLUDE_UPPER : includeUpper;
        this.docValues = docValues == null ? DEFAULT_DOC_VALUES : docValues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query doQuery(SingleColumnMapper<?> mapper, Analyzer analyzer) {

        // Check doc values
        if (docValues && !mapper.docValues) {
            throw new IndexException("Field '{}' does not support doc_values", mapper.field);
        }

        Class<?> clazz = mapper.base;
        Query query;
        if (clazz == String.class) {
            String start = (String) mapper.base(field, lower);
            String stop = (String) mapper.base(field, upper);
            query = query(start, stop);
        } else if (clazz == Integer.class) {
            Integer start = (Integer) mapper.base(field, lower);
            Integer stop = (Integer) mapper.base(field, upper);
            query = query(start, stop);
        } else if (clazz == Long.class) {
            Long start = (Long) mapper.base(field, lower);
            Long stop = (Long) mapper.base(field, upper);
            query = query(start, stop);
        } else if (clazz == Float.class) {
            Float start = (Float) mapper.base(field, lower);
            Float stop = (Float) mapper.base(field, upper);
            query = query(start, stop);
        } else if (clazz == Double.class) {
            Double start = (Double) mapper.base(field, lower);
            Double stop = (Double) mapper.base(field, upper);
            query = query(start, stop);
        } else {
            throw new IndexException("Range queries are not supported by mapper '{}'", mapper);
        }
        return query;
    }

    private Query query(String start, String stop) {
        return docValues
               ? DocValuesRangeQuery.newBytesRefRange(field,
                                                      docValue(start),
                                                      docValue(stop),
                                                      includeLower,
                                                      includeUpper)
               : TermRangeQuery.newStringRange(field, start, stop, includeLower, includeUpper);
    }

    private Query query(Integer start, Integer stop) {
        return docValues
               ? DocValuesRangeQuery.newLongRange(field, docValue(start), docValue(stop), includeLower, includeUpper)
               : NumericRangeQuery.newIntRange(field, start, stop, includeLower, includeUpper);
    }

    private Query query(Long start, Long stop) {
        return docValues
               ? DocValuesRangeQuery.newLongRange(field, docValue(start), docValue(stop), includeLower, includeUpper)
               : NumericRangeQuery.newLongRange(field, start, stop, includeLower, includeUpper);
    }

    private Query query(Float start, Float stop) {
        return docValues
               ? DocValuesRangeQuery.newLongRange(field, docValue(start), docValue(stop), includeLower, includeUpper)
               : NumericRangeQuery.newFloatRange(field, start, stop, includeLower, includeUpper);
    }

    private Query query(Double start, Double stop) {
        return docValues
               ? DocValuesRangeQuery.newLongRange(field, docValue(start), docValue(stop), includeLower, includeUpper)
               : NumericRangeQuery.newDoubleRange(field, start, stop, includeLower, includeUpper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("lower", lower)
                                   .add("upper", upper)
                                   .add("includeLower", includeLower)
                                   .add("includeUpper", includeUpper)
                                   .add("docValues", docValues);
    }
}