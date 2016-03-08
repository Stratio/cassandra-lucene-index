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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;

/**
 * A {@link Condition} implementation that matches documents containing terms with a specified prefix.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PrefixCondition extends SingleColumnCondition {

    /** The field prefix to be matched. */
    public final String value;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link #DEFAULT_BOOST} is used as
     * default.
     * @param field the name of the field to be matched
     * @param value the field prefix to be matched
     */
    public PrefixCondition(Float boost, String field, String value) {
        super(boost, field);
        if (value == null) {
            throw new IndexException("Field value required");
        }
        this.value = value;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(SingleColumnMapper<?> mapper, Analyzer analyzer) {
        if (mapper.base == String.class) {
            Term term = new Term(field, value);
            Query query = new PrefixQuery(term);
            query.setBoost(boost);
            return query;
        } else {
            throw new IndexException("Prefix queries are not supported by mapper '%s'", mapper);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("field", field).add("value", value).toString();
    }
}