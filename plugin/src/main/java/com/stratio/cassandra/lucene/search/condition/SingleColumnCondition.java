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
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

/**
 * The abstract base class for queries.
 *
 * Known subclasses are: <ul> <li> {@link FuzzyCondition} <li> {@link MatchCondition} <li> {@link PhraseCondition} <li>
 * {@link PrefixCondition} <li> {@link RangeCondition} <li> {@link WildcardCondition} </ul>
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleColumnCondition extends SingleFieldCondition {

    /**
     * Abstract {@link SingleColumnCondition} builder receiving the boost to be used.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     */
    public SingleColumnCondition(Float boost, String field) {
        super(boost, field);
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public final Query query(Schema schema) {
        Mapper mapper = schema.getMapper(field);
        if (mapper == null) {
            throw new IndexException("No mapper found for field '%s'", field);
        } else if (!SingleColumnMapper.class.isAssignableFrom(mapper.getClass())) {
            throw new IndexException("Field '%s' requires a mapper of type '%s' but found '%s'",
                                     field,
                                     SingleColumnMapper.class.getSimpleName(),
                                     mapper);
        }
        return query((SingleColumnMapper<?>) mapper, schema.getAnalyzer());
    }

    /**
     * Returns the Lucene {@link Query} representation of this condition.
     *
     * @param mapper the mapper to be used
     * @param analyzer the {@link Schema} analyzer
     * @return the Lucene query
     */
    public abstract Query query(SingleColumnMapper<?> mapper, Analyzer analyzer);
}
