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

package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import javax.validation.constraints.NotNull;
import java.util.Collections;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @param <T> THe base type.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleColumnMapper<T> extends Mapper {

    /** The name of the mapped column. */
    public final String column;

    /** The Lucene type for this mapper. */
    public final Class<T> base;

    /**
     * Builds a new {@link SingleColumnMapper} supporting the specified types for indexing and clustering.
     *
     * @param field          The name of the field.
     * @param column         The name of the column to be mapped.
     * @param indexed        If the field supports searching.
     * @param sorted         If the field supports sorting.
     * @param analyzer       The name of the analyzer to be used.
     * @param base           The Lucene type for this mapper.
     * @param supportedTypes The supported Cassandra types for indexing.
     */
    public SingleColumnMapper(String field,
                              String column,
                              Boolean indexed,
                              Boolean sorted,
                              String analyzer,
                              Class<T> base,
                              AbstractType<?>... supportedTypes) {
        super(field,
              indexed,
              sorted,
              analyzer,
              Collections.singletonList(column == null ? field : column),
              supportedTypes);

        if (StringUtils.isWhitespace(column)) {
            throw new IndexException("Column must not be whitespace, but found '%s'", column);
        }

        this.column = column == null ? field : column;
        this.base = base;
    }

    /** {@inheritDoc} */
    @Override
    public void addFields(Document document, Columns columns) {
        for (Column<?> c : columns.getColumnsByName(column)) {
            String name = c.getFieldName();
            Object value = c.getComposedValue();
            addFields(document, name, value);
        }
    }

    /**
     * Adds the specified column name and value to the specified {@link Document}.
     *
     * @param document A {@link Document}.
     * @param name     The name of the column to be mapped.
     * @param value    The value of the column to be mapped.
     */
    private void addFields(Document document, String name, Object value) {
        if (value != null) {
            T b = base(name, value);
            if (indexed) {
                document.add(indexedField(name, b));
            }
            if (sorted) {
                document.add(sortedField(name, b));
            }
        }
    }

    /**
     * Returns the {@link Field} to search for the mapped column.
     *
     * @param name  The name of the column.
     * @param value The value of the column.
     * @return The {@link Field} to search for the mapped column.
     */
    public abstract Field indexedField(String name, T value);

    /**
     * Returns the {@link Field} to sort by the mapped column.
     *
     * @param name  The name of the column.
     * @param value The value of the column.
     * @return The {@link Field} to sort by the mapped column.
     */
    public abstract Field sortedField(String name, T value);

    /**
     * Returns the {@link Column} query value resulting from the mapping of the specified object.
     *
     * @param field The field name.
     * @param value The object to be mapped, never is {@code null}.
     * @return The {@link Column} index value resulting from the mapping of the specified object.
     */
    public final T base(String field, Object value) {
        return value == null ? null : doBase(field, value);
    }

    protected abstract T doBase(String field, @NotNull Object value);

    /** {@inheritDoc} */
    @Override
    protected Objects.ToStringHelper toStringHelper(Object self) {
        return super.toStringHelper(self).add("column", column);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).toString();
    }

}
