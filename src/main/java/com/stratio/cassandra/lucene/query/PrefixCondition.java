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
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperSingle;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Condition} implementation that matches documents containing terms with a specified prefix.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class PrefixCondition extends SingleFieldCondition {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The field prefix to be matched. */
    @JsonProperty("value")
    private final String value;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     *              weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *              #DEFAULT_BOOST} is used as default.
     * @param field The name of the field to be matched.
     * @param value The field prefix to be matched.
     */
    @JsonCreator
    public PrefixCondition(@JsonProperty("boost") Float boost,
                           @JsonProperty("field") String field,
                           @JsonProperty("value") String value) {
        super(boost);

        this.field = field;
        this.value = value;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {

        if (field == null || field.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name required");
        }
        if (value == null) {
            throw new IllegalArgumentException("Field value required");
        }

        ColumnMapperSingle<?> columnMapper = getMapper(schema, field);
        Class<?> clazz = columnMapper.baseClass();
        Query query;
        if (clazz == String.class) {
            Term term = new Term(field, value);
            query = new PrefixQuery(term);
        } else {
            String message = String.format("Prefix queries are not supported by %s mapper", clazz.getSimpleName());
            throw new UnsupportedOperationException(message);
        }
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("field", field).add("value", value).toString();
    }
}