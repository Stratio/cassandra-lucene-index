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
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * A {@link Condition} implementation that matches documents containing a particular sequence of terms.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class PhraseCondition extends SingleFieldCondition {

    /** The default umber of other words permitted between words in phrase. */
    public static final int DEFAULT_SLOP = 0;

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The phrase terms to be matched. */
    @JsonProperty("values")
    private final List<String> values;

    /** The number of other words permitted between words in phrase. */
    @JsonProperty("slop")
    private Integer slop;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost  The boost for this query clause. Documents matching this clause will (in addition to the normal
     *               weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *               #DEFAULT_BOOST} is used as default.
     * @param field  The name of the field to be matched.
     * @param values The phrase terms to be matched.
     * @param slop   The number of other words permitted between words in phrase.
     */
    @JsonCreator
    public PhraseCondition(@JsonProperty("boost") Float boost,
                           @JsonProperty("field") String field,
                           @JsonProperty("values") List<String> values,
                           @JsonProperty("slop") Integer slop) {
        super(boost);

        this.field = field;
        this.values = values;
        this.slop = slop == null ? DEFAULT_SLOP : slop;
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
        if (slop == null) {
            throw new IllegalArgumentException("Slop required");
        }
        if (slop < 0) {
            throw new IllegalArgumentException("Slop must be positive");
        }

        ColumnMapperSingle<?> columnMapper = getMapper(schema, field);
        Class<?> clazz = columnMapper.baseClass();
        if (clazz == String.class) {
            PhraseQuery query = new PhraseQuery();
            query.setSlop(slop);
            query.setBoost(boost);
            int count = 0;
            for (String value : values) {
                if (value != null) {
                    String analyzedValue = analyze(field, value, schema);
                    if (analyzedValue != null) {
                        Term term = new Term(field, analyzedValue);
                        query.add(term, count);
                    }
                }
                count++;
            }
            return query;
        } else {
            String message = String.format("Unsupported query %s for mapper %s", this, columnMapper);
            throw new UnsupportedOperationException(message);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("field", field).add("values", values).add("slop", slop).toString();
    }
}