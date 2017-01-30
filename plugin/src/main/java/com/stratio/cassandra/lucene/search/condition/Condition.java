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
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

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
     * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     */
    public Condition(Float boost) {
        this.boost = boost;
    }

    /**
     * Returns the Lucene {@link Query} representation of this condition.
     *
     * @param schema the schema to be used
     * @return the Lucene query
     */
    public final Query query(Schema schema) {
        Query query = doQuery(schema);
        return boost == null ? query : new BoostQuery(query, boost);
    }

    /**
     * Returns the Lucene {@link Query} representation of this condition without boost.
     *
     * @param schema the schema to be used
     * @return the Lucene query
     */
    protected abstract Query doQuery(Schema schema);

    /**
     * Returns the names of the involved fields.
     *
     * @return the names of the involved fields
     */
    public abstract Set<String> postProcessingFields();

    static BytesRef docValue(String value) {
        return value == null ? null : new BytesRef(value);
    }

    static Long docValue(Long value) {
        return value == null ? null : value;
    }

    static Long docValue(Integer value) {
        return value == null ? null : value.longValue();
    }

    static Long docValue(Float value) {
        return value == null ? null : docValue(NumericUtils.floatToSortableInt(value));
    }

    static Long docValue(Double value) {
        return value == null ? null : NumericUtils.doubleToSortableLong(value);
    }

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
