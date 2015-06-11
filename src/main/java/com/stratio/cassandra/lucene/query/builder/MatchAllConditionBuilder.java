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

import com.stratio.cassandra.lucene.query.MatchAllCondition;

/**
 * {@link ConditionBuilder} for building a new {@link MatchAllCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class MatchAllConditionBuilder extends ConditionBuilder<MatchAllCondition, MatchAllConditionBuilder> {

    /**
     * Creates a new {@link MatchAllConditionBuilder}.
     */
    public MatchAllConditionBuilder() {
    }

    /**
     * Returns the {@link MatchAllCondition} represented by this builder.
     *
     * @return The {@link MatchAllCondition} represented by this builder.
     */
    @Override
    public MatchAllCondition build() {
        return new MatchAllCondition(boost);
    }
}
