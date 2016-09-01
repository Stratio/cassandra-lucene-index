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
import com.stratio.cassandra.lucene.schema.mapping.TextMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.QueryBuilder;

/**
 * A {@link Condition} implementation that matches documents containing a value for a field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class MatchCondition extends SingleColumnCondition {

    /** The default use doc values option. */
    public static final boolean DEFAULT_DOC_VALUES = false;

    /** The value of the field to be matched. */
    public final Object value;

    /** If the generated query should use doc values. */
    public final boolean docValues;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @param docValues if the generated query should use doc values
     */
    public MatchCondition(Float boost, String field, Object value, Boolean docValues) {
        super(boost, field);
        if (value == null) {
            throw new IndexException("Field value required");
        }
        this.value = value;
        this.docValues = docValues == null ? DEFAULT_DOC_VALUES : docValues;
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(SingleColumnMapper<?> mapper, Analyzer analyzer) {

        // Check doc values
        if (docValues && !mapper.docValues) {
            throw new IndexException("Field '{}' does not support doc_values", mapper.field);
        }

        Class<?> clazz = mapper.base;
        Query query;
        if (clazz == String.class) {
            String base = (String) mapper.base(field, value);
            if (mapper instanceof TextMapper) {
                QueryBuilder queryBuilder = new QueryBuilder(analyzer);
                query = queryBuilder.createPhraseQuery(field, base, 0);
            } else {
                query = query(base);
            }
            if (query == null) {
                query = new BooleanQuery.Builder().build();
            }
        } else if (clazz == Integer.class) {
            query = query((Integer) mapper.base(field, value));
        } else if (clazz == Long.class) {
            query = query((Long) mapper.base(field, value));
        } else if (clazz == Float.class) {
            query = query((Float) mapper.base(field, value));
        } else if (clazz == Double.class) {
            query = query((Double) mapper.base(field, value));
        } else {
            throw new IndexException("Match queries are not supported by mapper '{}'", mapper);
        }
        return query;
    }

    private Query query(String value) {
        return docValues ? new DocValuesTermsQuery(field, value) : new TermQuery(new Term(field, value));
    }

    private Query query(Integer value) {
        if (docValues) {
            return new DocValuesNumbersQuery(field, docValue(value));
        } else {
            BytesRefBuilder ref = new BytesRefBuilder();
            NumericUtils.intToPrefixCoded(value, 0, ref);
            return new TermQuery(new Term(field, ref.toBytesRef()));
        }
    }

    private Query query(Long value) {
        if (docValues) {
            return new DocValuesNumbersQuery(field, docValue(value));
        } else {
            BytesRefBuilder ref = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(value, 0, ref);
            return new TermQuery(new Term(field, ref.toBytesRef()));
        }
    }

    private Query query(Float value) {
        return docValues
               ? new DocValuesNumbersQuery(field, docValue(value))
               : NumericRangeQuery.newFloatRange(field, value, value, true, true);
    }

    private Query query(Double value) {
        return docValues
               ? new DocValuesNumbersQuery(field, docValue(value))
               : NumericRangeQuery.newDoubleRange(field, value, value, true, true);
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("value", value).add("docValues", docValues);
    }
}