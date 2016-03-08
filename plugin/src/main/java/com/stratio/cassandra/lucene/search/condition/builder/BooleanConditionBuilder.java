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

package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.search.condition.BooleanCondition;
import com.stratio.cassandra.lucene.search.condition.Condition;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * {@link ConditionBuilder} for building a new {@link BooleanCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BooleanConditionBuilder extends ConditionBuilder<BooleanCondition, BooleanConditionBuilder> {

    /** The mandatory conditions. */
    @JsonProperty("must")
    private final List<ConditionBuilder<?, ?>> must = new ArrayList<>();

    /** The optional conditions. */
    @JsonProperty("should")
    private final List<ConditionBuilder<?, ?>> should = new ArrayList<>();

    /** The mandatory not conditions. */
    @JsonProperty("not")
    private final List<ConditionBuilder<?, ?>> not = new ArrayList<>();

    /**
     * Returns this builder with the specified mandatory conditions.
     *
     * @param conditionBuilders The mandatory conditions to be added.
     * @return this builder with the specified mandatory conditions.
     */
    public BooleanConditionBuilder must(ConditionBuilder<?, ?>... conditionBuilders) {
        must.addAll(Arrays.asList(conditionBuilders));
        return this;
    }

    /**
     * Returns this builder with the specified optional conditions.
     *
     * @param conditionBuilders The optional conditions to be added
     * @return this builder with the specified optional conditions
     */
    public BooleanConditionBuilder should(ConditionBuilder<?, ?>... conditionBuilders) {
        should.addAll(Arrays.asList(conditionBuilders));
        return this;
    }

    /**
     * Returns this builder with the specified mandatory not conditions.
     *
     * @param conditionBuilders The mandatory not conditions to be added
     * @return this builder with the specified mandatory not conditions
     */
    public BooleanConditionBuilder not(ConditionBuilder<?, ?>... conditionBuilders) {
        not.addAll(Arrays.asList(conditionBuilders));
        return this;
    }

    /**
     * Returns the {@link BooleanCondition} represented by this builder.
     *
     * @return a new boolean condition
     */
    @Override
    public BooleanCondition build() {
        List<Condition> mustConditions = must.stream().map(ConditionBuilder::build).collect(toList());
        List<Condition> shouldConditions = should.stream().map(ConditionBuilder::build).collect(toList());
        List<Condition> notConditions = not.stream().map(ConditionBuilder::build).collect(toList());
        return new BooleanCondition(boost, mustConditions, shouldConditions, notConditions);
    }
}
