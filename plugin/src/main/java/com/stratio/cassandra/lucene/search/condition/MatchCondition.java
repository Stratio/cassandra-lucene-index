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
import com.stratio.cassandra.lucene.schema.mapping.TextMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.QueryBuilder;

/**
 * A {@link Condition} implementation that matches documents containing a value for a field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class MatchCondition extends SingleColumnCondition {

    /** The value of the field to be matched. */
    public final Object value;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link #DEFAULT_BOOST} is used as
     * default.
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     */
    public MatchCondition(Float boost, String field, Object value) {
        super(boost, field);
        if (value == null) {
            throw new IndexException("Field value required");
        }
        this.value = value;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(SingleColumnMapper<?> mapper, Analyzer analyzer) {
        Class<?> clazz = mapper.base;
        Query query;
        if (clazz == String.class) {
            String base = (String) mapper.base(field, value);
            if (mapper instanceof TextMapper) {
                QueryBuilder queryBuilder = new QueryBuilder(analyzer);
                query = queryBuilder.createPhraseQuery(field, base, 0);
            } else {
                query = new TermQuery(new Term(field, base));
            }
            if (query == null) {
                query = new BooleanQuery.Builder().build();
            }
        } else if (clazz == Integer.class) {
            Integer base = (Integer) mapper.base(field, value);
            query = NumericRangeQuery.newIntRange(field, base, base, true, true);
        } else if (clazz == Long.class) {
            Long base = (Long) mapper.base(field, value);
            query = NumericRangeQuery.newLongRange(field, base, base, true, true);
        } else if (clazz == Float.class) {
            Float base = (Float) mapper.base(field, value);
            query = NumericRangeQuery.newFloatRange(field, base, base, true, true);
        } else if (clazz == Double.class) {
            Double base = (Double) mapper.base(field, value);
            query = NumericRangeQuery.newDoubleRange(field, base, base, true, true);
        } else {
            throw new IndexException("Match queries are not supported by mapper '%s'", mapper);
        }
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("value", value).toString();
    }
}