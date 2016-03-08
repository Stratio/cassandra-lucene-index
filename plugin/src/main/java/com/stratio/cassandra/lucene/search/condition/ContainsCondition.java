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

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.util.Arrays;

/**
 * A {@link Condition} implementation that matches documents containing a value for a field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ContainsCondition extends SingleColumnCondition {

    /** The name of the field to be matched. */
    public final String field;

    /** The value of the field to be matched. */
    public final Object[] values;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link #DEFAULT_BOOST} is used as
     * default.
     * @param field the name of the field to be matched
     * @param values the value of the field to be matched
     */
    public ContainsCondition(Float boost, String field, Object... values) {
        super(boost, field);

        if (values == null || values.length == 0) {
            throw new IndexException("Field values required");
        }

        this.field = field;
        this.values = values;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(SingleColumnMapper<?> mapper, Analyzer analyzer) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Object value : values) {
            MatchCondition condition = new MatchCondition(boost, field, value);
            builder.add(condition.query(mapper, analyzer), BooleanClause.Occur.SHOULD);
        }
        Query query = builder.build();
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("boost", boost)
                          .add("field", field)
                          .add("values", Arrays.toString(values))
                          .toString();
    }
}