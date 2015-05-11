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

import com.stratio.cassandra.lucene.query.LuceneCondition;

/**
 * {@link ConditionBuilder} for building a new {@link LuceneCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LuceneConditionBuilder extends ConditionBuilder<LuceneCondition, LuceneConditionBuilder> {

    /** The Lucene query syntax expression. */
    private final String query;

    /** The name of the field where the clauses will be applied by default. */
    private String defaultField;

    /**
     * Returns a new {@link LuceneConditionBuilder} with the specified query.
     *
     * @param query The Lucene query syntax expression.
     */
    protected LuceneConditionBuilder(String query) {
        this.query = query;
    }

    /**
     * Returns this builder with the specified default field name. This is the field where the clauses will be applied
     * by default.
     *
     * @param defaultField The name of the field where the clauses will be applied by default.
     * @return This builder with the specified name of the default field.
     */
    public LuceneConditionBuilder defaultField(String defaultField) {
        this.defaultField = defaultField;
        return this;
    }

    /**
     * Returns the {@link LuceneCondition} represented by this builder.
     *
     * @return The {@link LuceneCondition} represented by this builder.
     */
    @Override
    public LuceneCondition build() {
        return new LuceneCondition(boost, defaultField, query);
    }
}
