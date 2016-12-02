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
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class Mapper {

    /** A no-action analyzer for not tokenized {@link Mapper} implementations. */
    static final String KEYWORD_ANALYZER = StandardAnalyzers.KEYWORD.toString();

    static final List<Class<?>> TEXT_TYPES = Collections.singletonList(String.class);

    static final List<Class<?>> INTEGER_TYPES = Arrays.asList(
            String.class, Byte.class, Short.class, Integer.class, Long.class, BigInteger.class);

    static final List<Class<?>> NUMERIC_TYPES = Arrays.asList(String.class, Number.class);

    static final List<Class<?>> DATE_TYPES = Arrays.asList(
            String.class, Integer.class, Long.class, BigInteger.class, Date.class, UUID.class);

    static final List<Class<?>> NUMERIC_TYPES_WITH_DATE = Arrays.asList(String.class, Number.class, Date.class);

    static final List<Class<?>> PRINTABLE_TYPES = Arrays.asList(
            String.class, Number.class, UUID.class, Boolean.class, InetAddress.class);

    /** The store field in Lucene default option. */
    static final Store STORE = Store.NO;

    /** If the field must be validated when no specified. */
    static final boolean DEFAULT_VALIDATED = false;

    /** The name of the Lucene field. */
    public final String field;

    /** If the field produces doc values. */
    public final Boolean docValues;

    /** If the field must be validated. */
    public final Boolean validated;

    /** The name of the analyzer to be used. */
    public final String analyzer;

    /** The names of the columns to be mapped. */
    public final List<String> mappedColumns;

    /** The names of the columns to be mapped. */
    public final List<String> mappedCells;

    /** The supported column value data types. */
    public final List<Class<?>> supportedTypes;

    /**
     * Builds a new {@link Mapper} supporting the specified types for indexing.
     *
     * @param field the name of the field
     * @param docValues if the mapper supports doc values
     * @param validated if the field must be validated
     * @param analyzer the name of the analyzer to be used
     * @param mappedColumns the names of the columns to be mapped
     * @param supportedTypes the supported column value data types
     */
    protected Mapper(String field,
                     Boolean docValues,
                     Boolean validated,
                     String analyzer,
                     List<String> mappedColumns,
                     List<Class<?>> supportedTypes) {
        if (StringUtils.isBlank(field)) {
            throw new IndexException("Field name is required");
        }
        this.field = field;
        this.docValues = docValues;
        this.validated = validated == null ? DEFAULT_VALIDATED : validated;
        this.analyzer = analyzer;
        this.mappedColumns = mappedColumns.stream().filter(x -> x != null).collect(toList()); // Remove nulls
        this.mappedCells = this.mappedColumns.stream().map(Column::parseCellName).collect(toList());
        this.supportedTypes = supportedTypes;
    }

    /**
     * Returns the Lucene {@link IndexableField}s resulting from the mapping of the specified {@link Columns}.
     *
     * @param columns the columns
     * @return a list of indexable fields
     */
    public abstract List<IndexableField> indexableFields(Columns columns);

    /**
     * Validates the specified {@link Columns} if {#validated}.
     *
     * @param columns the columns to be validated
     */
    public final void validate(Columns columns) {
        if (validated) {
            indexableFields(columns);
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
     * Returns if this maps the specified cell.
     *
     * @param cell the cell name
     * @return {@code true} if this maps the column, {@code false} otherwise
     */
    public boolean mapsCell(String cell) {
        return mappedCells.stream().anyMatch(x -> x.equals(cell));
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
