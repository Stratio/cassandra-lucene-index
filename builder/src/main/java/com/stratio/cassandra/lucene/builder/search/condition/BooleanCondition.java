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
package com.stratio.cassandra.lucene.builder.search.condition;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link Condition} that matches documents matching boolean combinations of other queries, e.g. {@link
 * MatchCondition}s, {@link RangeCondition}s or other {@link BooleanCondition}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@SuppressWarnings("unused")
public class BooleanCondition extends Condition<BooleanCondition> {

    /** The mandatory conditions not participating in scoring. */
    @JsonProperty("filter")
    private List<Condition> filter;

    /** The mandatory conditions participating in scoring. */
    @JsonProperty("must")
    private List<Condition> must;

    /** The optional conditions participating in scoring. */
    @JsonProperty("should")
    private List<Condition> should;

    /** The mandatory not conditions not participating in scoring. */
    @JsonProperty("not")
    private List<Condition> not;

    /**
     * Returns this with the specified mandatory conditions not participating in scoring.
     *
     * @param conditions the filtering conditions to be added
     * @return this with the specified filtering conditions
     */
    public BooleanCondition filter(Condition... conditions) {
        filter = add(filter, conditions);
        return this;
    }

    /**
     * Returns this with the specified mandatory conditions participating in scoring.
     *
     * @param conditions the mandatory conditions to be added
     * @return this with the specified mandatory conditions
     */
    public BooleanCondition must(Condition... conditions) {
        must = add(must, conditions);
        return this;
    }

    /**
     * Returns this with the specified optional conditions participating in scoring.
     *
     * @param conditions the optional conditions to be added
     * @return this with the specified optional conditions
     */
    public BooleanCondition should(Condition... conditions) {
        should = add(should, conditions);
        return this;
    }

    /**
     * Returns this with the specified mandatory not conditions not participating in scoring.
     *
     * @param conditions the mandatory not conditions to be added
     * @return this with the specified mandatory not conditions
     */
    public BooleanCondition not(Condition... conditions) {
        not = add(not, conditions);
        return this;
    }
}
