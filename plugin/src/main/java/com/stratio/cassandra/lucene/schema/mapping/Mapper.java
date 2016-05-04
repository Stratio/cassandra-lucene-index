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

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.analysis.StandardAnalyzers;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.marshal.CollectionType.Kind;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.search.SortField;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.cassandra.db.marshal.CollectionType.Kind.LIST;
import static org.apache.cassandra.db.marshal.CollectionType.Kind.SET;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class Mapper {

    /** A no-action getAnalyzer for not tokenized {@link Mapper} implementations. */
    static final String KEYWORD_ANALYZER = StandardAnalyzers.KEYWORD.toString();

    /** The store field in Lucene default option. */
    static final Store STORE = Store.NO;

    /** If the field must be indexed when no specified. */
    public static final boolean DEFAULT_INDEXED = true;

    /** If the field must be sorted when no specified. */
    public static final boolean DEFAULT_SORTED = false;

    /** The name of the Lucene field. */
    public final String field;

    /** If the field must be indexed. */
    public final Boolean indexed;

    /** If the field must be sorted. */
    public final Boolean sorted;

    /** The name of the analyzer to be used. */
    public final String analyzer;

    /** The supported Cassandra types for indexing. */
    public final AbstractType<?>[] supportedTypes;

    /** The names of the columns to be mapped. */
    public final List<String> mappedColumns;

    /**
     * Builds a new {@link Mapper} supporting the specified types for indexing.
     *
     * @param field          The name of the Lucene field.
     * @param indexed        If the field supports searching.
     * @param sorted         If the field supports sorting.
     * @param analyzer       The name of the analyzer to be used.
     * @param mappedColumns  The names of the columns to be mapped.
     * @param supportedTypes The supported Cassandra types for indexing.
     */
    protected Mapper(String field,
                     Boolean indexed,
                     Boolean sorted,
                     String analyzer,
                     List<String> mappedColumns,
                     AbstractType<?>... supportedTypes) {
        if (StringUtils.isBlank(field)) {
            throw new IndexException("Field name is required");
        }
        this.field = field;
        this.indexed = indexed == null ? DEFAULT_INDEXED : indexed;
        this.sorted = sorted == null ? DEFAULT_SORTED : sorted;
        this.analyzer = analyzer;
        this.mappedColumns = mappedColumns;
        this.supportedTypes = supportedTypes;
    }

    /**
     * Adds to the specified {@link Document} the Lucene {@link org.apache.lucene.document.Field}s resulting from the
     * mapping of the specified {@link Columns}.
     *
     * @param document The {@link Document} where the {@link org.apache.lucene.document.Field} are going to be added.
     * @param columns  The {@link Columns}.
     */
    public abstract void addFields(Document document, Columns columns);

    /**
     * Returns the {@link SortField} resulting from the mapping of the specified object.
     *
     * @param name    The name of the sorting field.
     * @param reverse If the sort must be reversed.
     * @return The {@link SortField} resulting from the mapping of the specified object.
     */
    public abstract SortField sortField(String name, boolean reverse);

    /**
     * Returns {@code true} if the specified Cassandra type/marshaller is supported, {@code false} otherwise.
     *
     * @param type A Cassandra type/marshaller.
     * @return {@code true} if the specified Cassandra type/marshaller is supported, {@code false} otherwise.
     */
    protected boolean supports(final AbstractType<?> type) {
        AbstractType<?> checkedType = type;
        if (type.isCollection()) {
            if (type instanceof MapType<?, ?>) {
                checkedType = ((MapType<?, ?>) type).getValuesType();
            } else if (type instanceof ListType<?>) {
                checkedType = ((ListType<?>) type).getElementsType();
            } else if (type instanceof SetType) {
                checkedType = ((SetType<?>) type).getElementsType();
            }
        }

        if (type instanceof ReversedType) {
            ReversedType<?> reversedType = (ReversedType<?>) type;
            checkedType = reversedType.baseType;
        }

        for (AbstractType<?> n : supportedTypes) {
            if (checkedType.getClass() == n.getClass()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Validates this {@link Mapper} against the specified {@link CFMetaData}.
     *
     * @param metadata A column family {@link CFMetaData}.
     */
    public final void validate(CFMetaData metadata) {
        for (String column : mappedColumns) {
            validate(metadata, column);
        }
    }

    /**
     * Validates this {@link Mapper} against the specified column.
     *
     * @param metadata A column family {@link CFMetaData}.
     * @param column   The name of the column to be validated.
     */
    private void validate(CFMetaData metadata, String column) {
        ByteBuffer columnName = UTF8Type.instance.decompose(column);
        ColumnDefinition columnDefinition = metadata.getColumnDefinition(columnName);
        if (columnDefinition == null) {
            throw new IndexException("No column definition '%s' for mapper '%s'", column, field);
        }
        validate(columnDefinition, column);
    }

    private void validate(ColumnDefinition columnDefinition, String column) {
        if (columnDefinition.isStatic()) {
            throw new IndexException("Lucene indexes are not allowed on static columns as '%s'", column);
        }
        validate(columnDefinition.type, column);
    }

    private void validate(AbstractType<?> type, String column) {

        // Check type
        if (!supports(type)) {
            throw new IndexException("'%s' is not supported by mapper '%s'", type, field);
        }

        // Avoid sorting in lists and sets
        if (type.isCollection() && sorted) {
            Kind kind = ((CollectionType<?>) type).kind;
            if (kind == SET) {
                throw new IndexException("'%s' can't be sorted because it's a set", column);
            } else if (kind == LIST) {
                throw new IndexException("'%s' can't be sorted because it's a list", column);
            }
        }
    }

    /**
     * Returns if the specified {@link Columns} contains the mapped columns.
     *
     * @param columns A {@link Columns}.
     * @return {@code true} if the specified {@link Columns} contains the mapped columns, {@code false} otherwise.
     */
    public final boolean maps(Columns columns) {
        for (String columnName : mappedColumns) {
            Columns mapperColumns = columns.getColumnsByName(columnName);
            if (mapperColumns.isEmpty()) {
                return false;
            }
            for (Column<?> column : mapperColumns) {
                if (column.isCollection()) {
                    return false;
                }
            }
        }
        return true;
    }

    protected Objects.ToStringHelper toStringHelper(Object self) {
        return Objects.toStringHelper(self).add("field", field).add("indexed", indexed).add("sorted", sorted);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).toString();
    }
}
