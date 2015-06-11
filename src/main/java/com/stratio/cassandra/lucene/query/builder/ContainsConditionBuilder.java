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

import com.stratio.cassandra.lucene.query.ContainsCondition;

/**
 * {@link ConditionBuilder} for building a new {@link ContainsCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ContainsConditionBuilder extends ConditionBuilder<ContainsCondition, ContainsConditionBuilder> {

    /** The name of the field to be matched. */
    private final String field;

    /** The value of the field to be matched. */
    private final Object[] values;

    /**
     * Creates a new {@link ContainsConditionBuilder} for the specified field and value.
     *
     * @param field  The name of the field to be matched.
     * @param values The values of the field to be matched.
     */
    public ContainsConditionBuilder(String field, Object... values) {
        this.field = field;
        this.values = values;
    }

    /**
     * Returns the {@link ContainsCondition} represented by this builder.
     *
     * @return The {@link ContainsCondition} represented by this builder.
     */
    @Override
    public ContainsCondition build() {
        return new ContainsCondition(boost, field, values);
    }
}
