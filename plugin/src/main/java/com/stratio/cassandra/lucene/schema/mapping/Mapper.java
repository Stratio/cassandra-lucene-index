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

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.analysis.StandardAnalyzers;
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
    public static final Store STORE = Store.NO;

    /** If the field must be indexed when no specified. */
    public static final boolean DEFAULT_INDEXED = true;

    /** If the field must be sorted when no specified. */
    public static final boolean DEFAULT_SORTED = false;

    /** If the field must be validated when no specified. */
    public static final boolean DEFAULT_VALIDATED = false;

    /** The name of the Lucene field. */
    public final String field;

    /** If the field must be indexed. */
    public final Boolean indexed;

    /** If the field must be sorted. */
    public final Boolean sorted;

    /** If the field must be validated. */
    public final Boolean validated;

    /** The name of the analyzer to be used. */
    public final String analyzer;

    /** The supported Cassandra types for indexing. */
    public final AbstractType<?>[] supportedTypes;

    /** The names of the columns to be mapped. */
    public final List<String> mappedColumns;

    /**
     * Builds a new {@link Mapper} supporting the specified types for indexing.
     *
     * @param field the name of the field
     * @param indexed if the field supports searching
     * @param sorted if the field supports sorting
     * @param validated if the field must be validated
     * @param analyzer the name of the analyzer to be used
     * @param mappedColumns the names of the columns to be mapped
     * @param supportedTypes the supported Cassandra types for indexing
     */
    protected Mapper(String field,
                     Boolean indexed,
                     Boolean sorted,
                     Boolean validated,
                     String analyzer,
                     List<String> mappedColumns,
                     AbstractType<?>... supportedTypes) {
        if (StringUtils.isBlank(field)) {
            throw new IndexException("Field name is required");
        }
        this.field = field;
        this.indexed = indexed == null ? DEFAULT_INDEXED : indexed;
        this.sorted = sorted == null ? DEFAULT_SORTED : sorted;
        this.validated = validated == null ? DEFAULT_VALIDATED : validated;
        this.analyzer = analyzer;
        this.mappedColumns = mappedColumns;
        this.supportedTypes = supportedTypes;
    }

    /**
     * Adds to the specified {@link Document} the Lucene {@link org.apache.lucene.document.Field}s resulting from the
     * mapping of the specified {@link Columns}.
     *
     * @param document the {@link Document} where the fields are going to be added
     * @param columns the columns
     */
    public abstract void addFields(Document document, Columns columns);

    /**
     * Validates the specified {@link Columns} if {#validated}.
     *
     * @param columns the columns to be validated
     */
    public final void validate(Columns columns) {
        if (validated) {
            addFields(new Document(), columns);
        }
    }

    /**
     * Returns the {@link SortField} resulting from the mapping of the specified object.
     *
     * @param name the name of the sorting field
     * @param reverse {@code true} the sort must be reversed, {@code false} otherwise
     * @return the sort field
     */
    public abstract SortField sortField(String name, boolean reverse);

    /**
     * Returns if the specified Cassandra type/marshaller is supported.
     *
     * @param type a Cassandra type/marshaller
     * @return {@code true} if {@code type}, {@code false} otherwise.
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
            return supports(checkedType);
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
     * @param metadata the column family metadata
     */
    public final void validate(CFMetaData metadata) {
        for (String column : mappedColumns) {
            validate(metadata, column);
        }
    }

    /**
     * Finds the child {@link AbstractType} by its name.
     *
     * @param parent the parent type
     * @param childName the name of the child type
     * @return the child type, or {@code null} if it doesn't exist
     */
    private AbstractType<?> findChildType(AbstractType<?> parent, String childName) {
        if (parent instanceof UserType) {
            UserType userType = (UserType) parent;
            for (int i = 0; i < userType.fieldNames().size(); i++) {
                if (userType.fieldNameAsString(i).equals(childName)) {
                    return userType.fieldType(i);
                }
            }
        } else if (parent instanceof TupleType) {
            TupleType tupleType = (TupleType) parent;
            for (Integer i = 0; i < tupleType.size(); i++) {
                if (i.toString().equals(childName)) {
                    return tupleType.type(i);
                }
            }
        } else if (parent.isCollection()) {
            CollectionType<?> collType = (CollectionType<?>) parent;
            switch (collType.kind) {
                case SET:
                    return findChildType(collType.nameComparator(), childName);
                case LIST:
                    return findChildType(collType.valueComparator(), childName);
                case MAP:
                    return findChildType(collType.valueComparator(), childName);
                default:
                    break;
            }
        }
        return null;
    }

    /**
     * Validates this {@link Mapper} against the specified tuple type column.
     *
     * @param metadata the column family metadata
     * @param column the name of the tuple column to be validated
     */
    private void validateTuple(CFMetaData metadata, String column) {

        String[] names = column.split(Column.UDT_PATTERN);
        int numMatches = names.length;

        ByteBuffer parentColName = UTF8Type.instance.decompose(names[0]);
        ColumnDefinition parentCD = metadata.getColumnDefinition(parentColName);
        if (parentCD == null) {
            throw new IndexException("No column definition '%s' for mapper '%s'", names[0], field);
        }

        if (parentCD.isStatic()) {
            throw new IndexException("Lucene indexes are not allowed on static columns as '%s'", column);
        }
        AbstractType<?> actualType = parentCD.type;
        String columnIterator = names[0];
        for (int i = 1; i < names.length; i++) {
            columnIterator += Column.UDT_SEPARATOR + names[i];
            actualType = findChildType(actualType, names[i]);
            if (actualType == null) {
                throw new IndexException("No column definition '%s' for mapper '%s'", columnIterator, field);
            }
            if (i == (numMatches - 1)) {
                validate(actualType, columnIterator);
            }
        }
    }

    /**
     * Validates this {@link Mapper} against the specified column.
     *
     * @param metadata the column family metadata
     * @param column the name of the column to be validated
     */
    private void validate(CFMetaData metadata, String column) {
        if (Column.isTuple(column)) {
            validateTuple(metadata, column);
        } else {
            ByteBuffer columnName = UTF8Type.instance.decompose(column);
            ColumnDefinition columnDefinition = metadata.getColumnDefinition(columnName);
            if (columnDefinition == null) {
                throw new IndexException("No column definition '%s' for mapper '%s'", column, field);
            }
            validate(columnDefinition, column);
        }
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
     * Returns if this maps the specified column definition.
     *
     * @param column the column definition
     * @return {@code true} if this maps the column, {@code false} otherwise
     */
    public boolean maps(ColumnDefinition column) {
        String name = column.name.toString();
        return mappedColumns.stream().anyMatch(x -> x.equals(name));
    }

    protected MoreObjects.ToStringHelper toStringHelper(Object self) {
        return MoreObjects.toStringHelper(self)
                          .add("field", field)
                          .add("indexed", indexed)
                          .add("sorted", sorted)
                          .add("validated", validated);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).toString();
    }
}
