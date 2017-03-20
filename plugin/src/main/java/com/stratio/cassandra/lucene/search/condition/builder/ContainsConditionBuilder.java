/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
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

import com.stratio.cassandra.lucene.search.condition.ContainsCondition;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link ContainsCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ContainsConditionBuilder extends ConditionBuilder<ContainsCondition, ContainsConditionBuilder> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The value of the field to be matched. */
    @JsonProperty("values")
    private final Object[] values;

    /**
     * Creates a new {@link ContainsConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param values the values of the field to be matched
     */
    @JsonCreator
    public ContainsConditionBuilder(@JsonProperty("field") String field, @JsonProperty("values") Object... values) {
        this.field = field;
        this.values = values;
    }

    /**
     * Returns the {@link ContainsCondition} represented by this builder.
     *
     * @return a new contains condition
     */
    @Override
    public ContainsCondition build() {
        return new ContainsCondition(boost, field, values);
    }
}
