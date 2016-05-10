/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
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
package com.stratio.cassandra.lucene.search.sort;

import static org.apache.lucene.search.SortField.*;

import java.util.*;

import org.apache.commons.lang3.*;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.*;
import com.stratio.cassandra.lucene.schema.*;
import com.stratio.cassandra.lucene.schema.column.*;
import com.stratio.cassandra.lucene.schema.mapping.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SimpleSortField extends SortField {

    /** The name of field to sortFields by. */
    public final String field;

    /**
     * Returns a new {@link SortField}.
     *
     * @param field the name of field to sort by
     * @param reverse {@code true} if natural order should be reversed, {@code false} otherwise
     */
    public SimpleSortField(String field, Boolean reverse) {
        super(reverse);

        if (field == null || StringUtils.isBlank(field)) {
            throw new IndexException("Field name required");
        }
        this.field = field;

    }

    /**
     * Returns the name of field to sort by.
     *
     * @return The name of field to sort by.
     */
    public String getField() {
        return field;
    }

    /**
     * Returns the Lucene {@link org.apache.lucene.search.SortField} representing this {@link SortField}.
     *
     * @param schema The {@link Schema} to be used.
     * @return the Lucene {@link org.apache.lucene.search.SortField} representing this {@link SortField}.
     */
    @Override
    public org.apache.lucene.search.SortField sortField(Schema schema) {
        if (field.equalsIgnoreCase("score")) {
            return FIELD_SCORE;
        }
        Mapper mapper = schema.getMapper(field);
        if (mapper == null) {
            throw new IndexException("No mapper found for sortFields field '%s'", field);
        } else if (!mapper.sorted) {
            throw new IndexException("Mapper '%s' is not sorted", mapper.field);
        } else {
            return mapper.sortField(field, reverse);
        }
    }

    /**
     * Returns a Java {@link Comparator} for {@link Columns} with the same logic as this {@link SortField}.
     *
     * @param schema The used {@link Schema}.
     * @return A Java {@link Comparator} for {@link Columns} with the same logic as this {@link SortField}.
     */
    public Comparator<Columns> comparator(Schema schema) {
        final SingleColumnMapper mapper = schema.getSingleColumnMapper(field);
        return new Comparator<Columns>() {
            public int compare(Columns o1, Columns o2) {
                return SimpleSortField.this.compare(mapper, o1, o2);
            }
        };
    }

    protected int compare(SingleColumnMapper mapper, Columns o1, Columns o2) {

        if (o1 == null) {
            return o2 == null ? 0 : 1;
        } else if (o2 == null) {
            return -1;
        }

        String column = mapper.getColumn();
        Column<?> column1 = o1.getColumnsByFullName(column).getFirst();
        Column<?> column2 = o2.getColumnsByFullName(column).getFirst();
        Comparable base1 = column1 == null ? null : mapper.base(column, column1.getComposedValue());
        Comparable base2 = column2 == null ? null : mapper.base(column, column2.getComposedValue());

        return compare(base1, base2);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("field", field).add("reverse", reverse).toString();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleSortField otherSimpleSortField = (SimpleSortField) o;
        return reverse == otherSimpleSortField.reverse && field.equals(otherSimpleSortField.field);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (reverse ? 1 : 0);
        return result;
    }
}
