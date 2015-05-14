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

import com.stratio.cassandra.lucene.query.PhraseCondition;

import java.util.List;

/**
 * {@link ConditionBuilder} for building a new {@link PhraseCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class PhraseConditionBuilder extends ConditionBuilder<PhraseCondition, PhraseConditionBuilder> {

    /** The name of the field to be matched. */
    private final String field;

    /** The phrase terms to be matched. */
    private final String[] values;

    /** The number of other words permitted between words in phrase. */
    private Integer slop;

    /**
     * Returns a new {@link PhraseConditionBuilder} with the specified field name and values to be matched.
     *
     * @param field  The name of the field to be matched.
     * @param values The phrase terms to be matched.
     */
    protected PhraseConditionBuilder(String field, List<String> values) {
        this.field = field;
        this.values = values.toArray(new String[values.size()]);
    }

    /**
     * Returns a new {@link PhraseConditionBuilder} with the specified field name and values to be matched.
     *
     * @param field  The name of the field.
     * @param values The phrase terms to be matched.
     */
    public PhraseConditionBuilder(String field, String... values) {
        this.field = field;
        this.values = values;
    }

    /**
     * Returns this builder with the specified slop. Slop is the number of other words permitted between words in
     * phrase.
     *
     * @param slop The number of other words permitted between words in phrase to set.
     * @return this builder with the specified slop.
     */
    public PhraseConditionBuilder slop(Integer slop) {
        this.slop = slop;
        return this;
    }

    /**
     * Returns the {@link PhraseCondition} represented by this builder.
     *
     * @return The {@link PhraseCondition} represented by this builder.
     */
    @Override
    public PhraseCondition build() {
        return new PhraseCondition(boost, field, values, slop);
    }
}
