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
import com.stratio.cassandra.lucene.schema.analysis.StandardAnalyzers;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.List;

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

    /** If the field must be validated when no specified. */
    public static final boolean DEFAULT_VALIDATED = false;

    /** The name of the Lucene field. */
    public final String field;

    /** If the field produces doc values. */
    public final Boolean docValues;

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
     * @param docValues if the mapper supports doc values
     * @param validated if the field must be validated
     * @param analyzer the name of the analyzer to be used
     * @param mappedColumns the names of the columns to be mapped
     * @param supportedTypes the supported Cassandra types for indexing
     */
    protected Mapper(String field,
                     Boolean docValues,
                     Boolean validated,
                     String analyzer,
                     List<String> mappedColumns,
                     AbstractType<?>... supportedTypes) {
        if (StringUtils.isBlank(field)) {
            throw new IndexException("Field name is required");
        }
        this.field = field;
        this.docValues = docValues;
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
     * @param type the parent type
     * @param childName the name of the child type
     * @return the child type, or {@code null} if it doesn't exist
     */
    private AbstractType<?> findChildType(AbstractType<?> type, String childName) {
        AbstractType<?> parent = (type instanceof ReversedType) ? ((ReversedType<?>) type).baseType : type;
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
            throw new IndexException("No column definition '{}' for mapper '{}'", names[0], field);
        }

        if (parentCD.isStatic()) {
            throw new IndexException("Lucene indexes are not allowed on static columns as '{}'", column);
        }
        AbstractType<?> actualType = parentCD.type;
        String columnIterator = names[0];
        for (int i = 1; i < names.length; i++) {
            columnIterator += Column.UDT_SEPARATOR + names[i];
            actualType = findChildType(actualType, names[i]);
            if (actualType == null) {
                throw new IndexException("No column definition '{}' for mapper '{}'", columnIterator, field);
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
                throw new IndexException("No column definition '{}' for mapper '{}'", column, field);
            }
            validate(columnDefinition, column);
        }
    }

    private void validate(ColumnDefinition columnDefinition, String column) {
        if (columnDefinition.isStatic()) {
            throw new IndexException("Lucene indexes are not allowed on static columns as '{}'", column);
        }
        validate(columnDefinition.type, column);
    }

    private void validate(AbstractType<?> type, String column) {
        // Check type
        if (!supports(type)) {
            throw new IndexException("Type '{}' in column '{}' is not supported by mapper '{}'", type, column, field);
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

    void validateTerm(String name, BytesRef term) {
        int maxSize = IndexWriter.MAX_TERM_LENGTH;
        int size = term.length;
        if (size > maxSize) {
            throw new IndexException("Discarding immense term in field='{}', " +
                                     "Lucene only allows terms with at most " +
                                     "{} bytes in length; got {} bytes: {}...",
                                     name, maxSize, size, term.utf8ToString().substring(0, 10));
        }
    }

    protected MoreObjects.ToStringHelper toStringHelper(Object self) {
        return MoreObjects.toStringHelper(self).add("field", field).add("validated", validated);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).toString();
    }
}
