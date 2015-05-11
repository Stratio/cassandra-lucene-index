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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * A {@link Condition} implementation that matches documents containing a value for a
 * field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ContainsCondition extends SingleFieldCondition {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The value of the field to be matched. */
    @JsonProperty("values")
    private List<Object> values;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost  The boost for this query clause. Documents matching this clause will (in addition to the normal
     *               weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *               #DEFAULT_BOOST} is used as default.
     * @param field  The name of the field to be matched.
     * @param values The value of the field to be matched.
     */
    @JsonCreator
    public ContainsCondition(@JsonProperty("boost") Float boost,
                             @JsonProperty("field") String field,
                             @JsonProperty("values") List<Object> values) {
        super(boost);
        this.field = field;
        this.values = values;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {
        if (field == null || field.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name required");
        }
        if (values == null) {
            throw new IllegalArgumentException("Field values required");
        }

        BooleanQuery query = new BooleanQuery();
        for (Object value : values) {
            Condition condition = new MatchCondition(boost, field, value);
            query.add(condition.query(schema), BooleanClause.Occur.SHOULD);
        }
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("field", field).add("values", values).toString();
    }
}