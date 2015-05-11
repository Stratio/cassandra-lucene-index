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
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.BooleanCondition;
import com.stratio.cassandra.lucene.query.Condition;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link ConditionBuilder} for building a new {@link BooleanCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class BooleanConditionBuilder extends ConditionBuilder<BooleanCondition, BooleanConditionBuilder> {

    /** The mandatory conditions */
    private List<Condition> must;

    /** The optional conditions */
    private List<Condition> should;

    /** The mandatory not conditions */
    private List<Condition> not;

    /**
     * Returns a new {@link BooleanConditionBuilder}.
     */
    BooleanConditionBuilder() {
    }

    /**
     * Returns this builder with the specified mandatory conditions.
     *
     * @param conditionBuilders The mandatory conditions to be added.
     * @return this builder with the specified mandatory conditions.
     */
    public BooleanConditionBuilder must(ConditionBuilder... conditionBuilders) {
        if (must == null) {
            must = new ArrayList<>(conditionBuilders.length);
        }
        for (ConditionBuilder conditionBuilder : conditionBuilders) {
            must.add(conditionBuilder.build());
        }
        return this;
    }

    /**
     * Returns this builder with the specified optional conditions.
     *
     * @param conditionBuilders The optional conditions to be added.
     * @return this builder with the specified optional conditions.
     */
    public BooleanConditionBuilder should(ConditionBuilder... conditionBuilders) {
        if (should == null) {
            should = new ArrayList<>(conditionBuilders.length);
        }
        for (ConditionBuilder conditionBuilder : conditionBuilders) {
            should.add(conditionBuilder.build());
        }
        return this;
    }

    /**
     * Returns this builder with the specified mandatory not conditions.
     *
     * @param conditionBuilders The mandatory not conditions to be added.
     * @return this builder with the specified mandatory not conditions.
     */
    public BooleanConditionBuilder not(ConditionBuilder... conditionBuilders) {
        if (not == null) {
            not = new ArrayList<>(conditionBuilders.length);
        }
        for (ConditionBuilder conditionBuilder : conditionBuilders) {
            not.add(conditionBuilder.build());
        }
        return this;
    }

    /**
     * Returns the {@link BooleanCondition} represented by this builder.
     *
     * @return The {@link BooleanCondition} represented by this builder.
     */
    @Override
    public BooleanCondition build() {
        return new BooleanCondition(boost, must, should, not);
    }
}
