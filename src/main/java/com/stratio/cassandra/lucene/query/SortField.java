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
import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import org.apache.cassandra.db.marshal.AbstractType;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * A sorting for a field of a search.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SortField {

    /** The default reverse option. */
    public static final boolean DEFAULT_REVERSE = false;

    /** The name of field to sortFields by. */
    @JsonProperty("field")
    private final String field;

    /** {@code true} if natural order should be reversed. */
    @JsonProperty("reverse")
    private final boolean reverse;

    /**
     * Returns a new {@link SortField}.
     *
     * @param field   The name of field to sortFields by.
     * @param reverse {@code true} if natural order should be reversed.
     */
    @JsonCreator
    public SortField(@JsonProperty("field") String field, @JsonProperty("reverse") Boolean reverse) {
        this.field = field;
        this.reverse = reverse == null ? DEFAULT_REVERSE : reverse;
    }

    /**
     * Returns the Lucene {@link org.apache.lucene.search.SortField} representing this {@link SortField}.
     *
     * @param schema The {@link Schema} to be used.
     * @return the Lucene {@link org.apache.lucene.search.SortField} representing this {@link SortField}.
     */
    public org.apache.lucene.search.SortField sortField(Schema schema) {
        if (field == null || field.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name required");
        }
        ColumnMapper columnMapper = schema.getMapper(field);
        if (columnMapper == null) {
            throw new IllegalArgumentException("No mapper found for sortFields field " + field);
        } else {
            return columnMapper.sortField(field, reverse);
        }
    }

    /**
     * Returns a Java {@link Comparator} for {@link Columns} with the same logic as this {@link SortField}.
     *
     * @return A Java {@link Comparator} for {@link Columns} with the same logic as this {@link SortField}.
     */
    public Comparator<Columns> comparator() {
        return new Comparator<Columns>() {
            public int compare(Columns o1, Columns o2) {

                if (o1 == null) {
                    return o2 == null ? 0 : 1;
                }
                if (o2 == null) {
                    return -1;
                }

                Column column1 = o1.getColumn(field);
                Column column2 = o2.getColumn(field);

                if (column1 == null) {
                    return column2 == null ? 0 : 1;
                }
                if (column2 == null) {
                    return -1;
                }

                AbstractType<?> type = column1.getType();
                ByteBuffer value1 = column1.getDecomposedValue();
                ByteBuffer value2 = column2.getDecomposedValue();
                return reverse ? type.compare(value2, value1) : type.compare(value1, value2);
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("field", field).add("reverse", reverse).toString();
    }
}
