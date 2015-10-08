/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.search.sort;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;

import static org.apache.lucene.search.SortField.FIELD_SCORE;

/**
 * A sorting for a field of a search.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SortField {

    /** The default reverse option. */
    public static final boolean DEFAULT_REVERSE = false;

    /** The name of field to sortFields by. */
    public final String field;

    /** {@code true} if natural order should be reversed. */
    public final boolean reverse;

    /**
     * Returns a new {@link SortField}.
     *
     * @param field   The name of field to sort by.
     * @param reverse {@code true} if natural order should be reversed.
     */
    public SortField(String field, Boolean reverse) {

        if (field == null || StringUtils.isBlank(field)) {
            throw new IndexException("Field name required");
        }

        this.field = field;
        this.reverse = reverse == null ? DEFAULT_REVERSE : reverse;
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
     * Returns {@code true} if natural order should be reversed.
     *
     * @return {@code true} if natural order should be reversed.
     */
    public boolean isReverse() {
        return reverse;
    }

    /**
     * Returns the Lucene {@link org.apache.lucene.search.SortField} representing this {@link SortField}.
     *
     * @param schema The {@link Schema} to be used.
     * @return the Lucene {@link org.apache.lucene.search.SortField} representing this {@link SortField}.
     */
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
     * @return A Java {@link Comparator} for {@link Columns} with the same logic as this {@link SortField}.
     */
    public Comparator<Columns> comparator() {
        return new Comparator<Columns>() {
            public int compare(Columns o1, Columns o2) {
                return SortField.this.compare(o1, o2);
            }
        };
    }

    protected int compare(Columns o1, Columns o2) {

        if (o1 == null) {
            return o2 == null ? 0 : 1;
        } else if (o2 == null) {
            return -1;
        }

        Column<?> column1 = o1.getColumnsByFullName(field).getFirst();
        Column<?> column2 = o2.getColumnsByFullName(field).getFirst();

        return compare(column1, column2);
    }

    protected int compare(Column<?> column1, Column<?> column2) {
        if (column1 == null) {
            return column2 == null ? 0 : 1;
        } else if (column2 == null) {
            return -1;
        } else if (reverse) {
            return column2.compareTo(column1);
        } else {
            return column1.compareTo(column2);
        }
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
        SortField sortField = (SortField) o;
        return reverse == sortField.reverse && field.equals(sortField.field);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (reverse ? 1 : 0);
        return result;
    }
}
