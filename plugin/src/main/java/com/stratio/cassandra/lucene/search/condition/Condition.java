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
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The abstract base class for queries.
 *
 * Known subclasses are: <ul> <li> {@link AllCondition} <li> {@link BitemporalCondition} <li> {@link ContainsCondition}
 * <li> {@link FuzzyCondition} <li> {@link MatchCondition} <li> {@link PhraseCondition} <li> {@link PrefixCondition}
 * <li> {@link RangeCondition} <li> {@link WildcardCondition} <li> {@link GeoDistanceCondition} <li> {@link
 * GeoBBoxCondition} </ul>
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class Condition {

    protected static final Logger logger = LoggerFactory.getLogger(Condition.class);

    /** The boost to be used. */
    public final Float boost;

    /**
     * Abstract {@link Condition} builder receiving the boost to be used.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     */
    public Condition(Float boost) {
        this.boost = boost;
    }

    /**
     * Returns the Lucene {@link Query} representation of this condition.
     *
     * @param schema the schema to be used
     * @return The Lucene query
     */
    public final Query query(Schema schema) {
        Query query = doQuery(schema);
        if (boost != null) {
            query.setBoost(boost);
        }
        return query;
    }

    /**
     * Returns the Lucene {@link Query} representation of this condition without boost.
     *
     * @param schema the schema to be used
     * @return The Lucene query
     */
    protected abstract Query doQuery(Schema schema);

    protected MoreObjects.ToStringHelper toStringHelper(Object o) {
        return MoreObjects.toStringHelper(o).add("boost", boost);
    }

    protected abstract MoreObjects.ToStringHelper toStringHelper();

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper().toString();
    }
}
