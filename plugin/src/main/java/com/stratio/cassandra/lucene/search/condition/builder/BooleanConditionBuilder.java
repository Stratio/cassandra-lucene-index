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
package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.search.condition.BooleanCondition;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * {@link ConditionBuilder} for building a new {@link BooleanCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BooleanConditionBuilder extends ConditionBuilder<BooleanCondition, BooleanConditionBuilder> {

    /** The mandatory conditions not participating in scoring. */
    @JsonProperty("filter")
    protected final List<ConditionBuilder<?, ?>> filter = new LinkedList<>();

    /** The mandatory conditions participating in scoring. */
    @JsonProperty("must")
    protected final List<ConditionBuilder<?, ?>> must = new LinkedList<>();

    /** The optional conditions participating in scoring. */
    @JsonProperty("should")
    protected final List<ConditionBuilder<?, ?>> should = new LinkedList<>();

    /** The mandatory not conditions not participating in scoring. */
    @JsonProperty("not")
    protected final List<ConditionBuilder<?, ?>> not = new LinkedList<>();

    /**
     * Returns this builder with the specified mandatory conditions not participating in scoring.
     *
     * @param builders the conditions to be added
     * @return this builder with the specified conditions
     */
    public BooleanConditionBuilder filter(ConditionBuilder<?, ?>... builders) {
        filter.addAll(Arrays.asList(builders));
        return this;
    }

    /**
     * Returns this builder with the specified mandatory conditions participating in scoring.
     *
     * @param builders the conditions to be added
     * @return this builder with the specified conditions
     */
    public BooleanConditionBuilder must(ConditionBuilder<?, ?>... builders) {
        must.addAll(Arrays.asList(builders));
        return this;
    }

    /**
     * Returns this builder with the specified optional conditions participating in scoring.
     *
     * @param builders the conditions to be added
     * @return this builder with the specified conditions
     */
    public BooleanConditionBuilder should(ConditionBuilder<?, ?>... builders) {
        should.addAll(Arrays.asList(builders));
        return this;
    }

    /**
     * Returns this builder with the specified mandatory not conditions not participating in scoring.
     *
     * @param builders the conditions to be added
     * @return this builder with the specified conditions
     */
    public BooleanConditionBuilder not(ConditionBuilder<?, ?>... builders) {
        not.addAll(Arrays.asList(builders));
        return this;
    }

    /**
     * Returns the {@link BooleanCondition} represented by this builder.
     *
     * @return a new boolean condition
     */
    @Override
    public BooleanCondition build() {
        return new BooleanCondition(boost,
                                    filter.stream().map(ConditionBuilder::build).collect(toList()),
                                    must.stream().map(ConditionBuilder::build).collect(toList()),
                                    should.stream().map(ConditionBuilder::build).collect(toList()),
                                    not.stream().map(ConditionBuilder::build).collect(toList()));
    }
}
