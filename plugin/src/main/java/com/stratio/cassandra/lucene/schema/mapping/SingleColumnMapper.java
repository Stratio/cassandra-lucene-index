/*
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
package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Optional;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @param <T> The base type.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleColumnMapper<T extends Comparable<T>> extends Mapper {

    /** The name of the mapped column. */
    public final String column;

    /** The Lucene type for this mapper. */
    public final Class<T> base;

    /**
     * Builds a new {@link SingleColumnMapper} supporting the specified types for indexing and clustering.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param docValues if the mapper supports doc values
     * @param validated if the field must be validated
     * @param analyzer the name of the analyzer to be used
     * @param base the Lucene type for this mapper
     * @param supportedTypes the supported Cassandra types for indexing
     */
    public SingleColumnMapper(String field,
                              String column,
                              Boolean docValues,
                              Boolean validated,
                              String analyzer,
                              Class<T> base,
                              AbstractType<?>... supportedTypes) {
        super(field, docValues,
              validated,
              analyzer,
              Collections.singletonList(column == null ? field : column), supportedTypes);

        if (StringUtils.isWhitespace(column)) {
            throw new IndexException("Column must not be whitespace, but found '{}'", column);
        }

        this.column = column == null ? field : column;
        this.base = base;
    }

    public String getColumn() {
        return column;
    }

    /** {@inheritDoc} */
    @Override
    public void addFields(Document document, Columns columns) {
        columns.getByMapperName(column).forEach(c -> addFields(document, c));
    }

    private <K> void addFields(Document document, Column<K> c) {
        String name = column.equals(field) ? c.getFullName() : c.getFieldName(field);
        K value = c.getValue();
        if (value != null) {
            T base = base(c);
            addIndexedFields(document, name, base);
            addSortedFields(document, name, base);
        }
    }

    /**
     * Returns the {@link Field} to search for the mapped column.
     *
     * @param document a {@link Document}
     * @param name the name of the column
     * @param value the value of the column
     */
    public abstract void addIndexedFields(Document document, String name, T value);

    /**
     * Returns the {@link Field} to sort by the mapped column.
     *
     * @param document a {@link Document}
     * @param name the name of the column
     * @param value the value of the column
     */
    public abstract void addSortedFields(Document document, String name, T value);

    /**
     * Returns the {@link Column} query value resulting from the mapping of the specified object.
     *
     * @param field the field name
     * @param value the object to be mapped, never is {@code null}
     * @return the {@link Column} index value resulting from the mapping of the specified object
     */
    public final T base(String field, Object value) {
        return value == null ? null : doBase(field, value);
    }

    /**
     * Returns the {@link Column} query value resulting from the mapping of the specified object.
     *
     * @param column the column
     * @param <K> the base type of the column
     * @return the {@link Column} index value resulting from the mapping of the specified object
     */
    public final <K> T base(Column<K> column) {
        return column == null ? null : column.getValue() == null ? null : doBase(column);
    }

    protected abstract T doBase(@NotNull String field, @NotNull Object value);

    protected final <K> T doBase(Column<K> column) {
        return doBase(column.getFieldName(field), column.getValue());
    }

    /** {@inheritDoc} */
    @Override
    protected MoreObjects.ToStringHelper toStringHelper(Object self) {
        return super.toStringHelper(self).add("column", column);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).toString();
    }

    /**
     * {@link SingleColumnMapper} that produces just a single field.
     *
     * @param <T> the base type
     */
    public abstract static class SingleFieldMapper<T extends Comparable<T>> extends SingleColumnMapper<T> {

        /**
         * Builds a new {@link SingleFieldMapper} supporting the specified types for indexing and clustering.
         *
         * @param field the name of the field
         * @param column the name of the column to be mapped
         * @param docValues if the mapper supports doc values
         * @param validated if the field must be validated
         * @param analyzer the name of the analyzer to be used
         * @param base the Lucene type for this mapper
         * @param supportedTypes the supported Cassandra types for indexing
         */
        public SingleFieldMapper(String field,
                                 String column,
                                 Boolean docValues,
                                 Boolean validated,
                                 String analyzer,
                                 Class<T> base,
                                 AbstractType<?>... supportedTypes) {
            super(field, column, docValues, validated, analyzer, base, supportedTypes);
        }

        /** {@inheritDoc} */
        @Override
        public void addIndexedFields(Document document, String name, T value) {
            indexedField(name, value).ifPresent(document::add);
        }

        /** {@inheritDoc} */
        @Override
        public void addSortedFields(Document document, String name, T value) {
            sortedField(name, value).ifPresent(document::add);
        }

        /**
         * Returns the {@link Field} to index by the mapped column.
         *
         * @param name the name of the column
         * @param value the value of the column
         * @return the field to sort by the mapped column
         */
        public abstract Optional<Field> indexedField(String name, T value);

        /**
         * Returns the {@link Field} to sort by the mapped column.
         *
         * @param name the name of the column
         * @param value the value of the column
         * @return the field to sort by the mapped column
         */
        public abstract Optional<Field> sortedField(String name, T value);
    }

}
