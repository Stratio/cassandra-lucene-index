/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.query;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.LinkedList;
import java.util.List;

/**
 * A {@link Condition} that matches documents matching boolean combinations of other queries, e.g. {@link
 * MatchCondition}s, {@link RangeCondition}s or other {@link BooleanCondition}s.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class BooleanCondition extends Condition {

    /** The mandatory conditions */
    @JsonProperty("must")
    private final List<Condition> must;

    /** The optional conditions */
    @JsonProperty("should")
    private final List<Condition> should;

    /** The mandatory not conditions */
    @JsonProperty("not")
    private final List<Condition> not;

    /**
     * Returns a new {@link BooleanCondition} compound by the specified {@link Condition}s.
     *
     * @param boost  The boost for this query clause. Documents matching this clause will (in addition to the normal
     *               weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *               #DEFAULT_BOOST} is used as default.
     * @param must   the mandatory {@link Condition}s.
     * @param should the optional {@link Condition}s.
     * @param not    the mandatory not {@link Condition}s.
     */
    @JsonCreator
    public BooleanCondition(@JsonProperty("boost") Float boost,
                            @JsonProperty("must") List<Condition> must,
                            @JsonProperty("should") List<Condition> should,
                            @JsonProperty("not") List<Condition> not) {
        super(boost);
        this.must = must == null ? new LinkedList<Condition>() : must;
        this.should = should == null ? new LinkedList<Condition>() : should;
        this.not = not == null ? new LinkedList<Condition>() : not;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {
        BooleanQuery luceneQuery = new BooleanQuery();
        luceneQuery.setBoost(boost);
        for (Condition query : must) {
            luceneQuery.add(query.query(schema), Occur.MUST);
        }
        for (Condition query : should) {
            luceneQuery.add(query.query(schema), Occur.SHOULD);
        }
        for (Condition query : not) {
            luceneQuery.add(query.query(schema), Occur.MUST_NOT);
        }
        return luceneQuery;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("must", must).add("should", should).add("not", not).toString();
    }
}
