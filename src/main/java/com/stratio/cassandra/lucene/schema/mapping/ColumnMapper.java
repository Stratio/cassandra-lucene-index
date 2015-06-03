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
package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.search.SortField;

import java.nio.ByteBuffer;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class ColumnMapper {

    /** A no-action getAnalyzer for not tokenized {@link ColumnMapper} implementations. */
    static final String KEYWORD_ANALYZER = PreBuiltAnalyzers.KEYWORD.toString();

    /** The store field in Lucene default option. */
    static final Store STORE = Store.NO;

    /** If the field must be indexed when no specified. */
    static final boolean DEFAULT_INDEXED = true;

    /** If the field must be sorted when no specified. */
    static final boolean DEFAULT_SORTED = true;

    /** The name of the mapper. */
    protected final String name;

    /** If the field must be indexed. */
    protected final Boolean indexed;

    /** If the field must be sorted. */
    protected final Boolean sorted;

    /** The supported Cassandra types for indexing. */
    private final AbstractType<?>[] supportedTypes;

    /**
     * Builds a new {@link ColumnMapper} supporting the specified types for indexing.
     *
     * @param name           The name of the mapper.
     * @param indexed        If the field supports searching.
     * @param sorted         If the field supports sorting.
     * @param supportedTypes The supported Cassandra types for indexing.
     */
    protected ColumnMapper(String name, Boolean indexed, Boolean sorted, AbstractType<?>... supportedTypes) {
        if (StringUtils.isBlank(name)) throw new IllegalArgumentException("Mapper name is required");
        this.name = name;
        this.indexed = indexed == null ? DEFAULT_INDEXED : indexed;
        this.sorted = sorted == null ? DEFAULT_SORTED : sorted;
        this.supportedTypes = supportedTypes;
    }

    /**
     * Returns {@code true} if the columns must be searchable, {@code false} otherwise.
     *
     * @return {@code true} if the columns must be searchable, {@code false} otherwise.
     */
    public boolean isIndexed() {
        return indexed;
    }

    /**
     * Returns {@code true} if the columns must be sortable, {@code false} otherwise.
     *
     * @return {@code true} if the columns must be sortable, {@code false} otherwise.
     */
    public boolean isSorted() {
        return sorted;
    }

    /**
     * Returns the name of the used {@link Analyzer}.
     *
     * @return The name of the used {@link Analyzer}.
     */
    public String getAnalyzer() {
        return KEYWORD_ANALYZER;
    }

    /**
     * Adds to the specified {@link Document} the Lucene {@link Field}s resulting from the mapping of the specified
     * {@link Columns}.
     *
     * @param document The {@link Document} where the {@link Field} are going to be added.
     * @param columns  The {@link Columns}.
     */
    public abstract void addFields(Document document, Columns columns);

    /**
     * Returns the {@link SortField} resulting from the mapping of the specified object.
     *
     * @param reverse If the sort must be reversed.
     * @return The {@link SortField} resulting from the mapping of the specified object.
     */
    public abstract SortField sortField(boolean reverse);

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
            ReversedType reversedType = (ReversedType) type;
            checkedType = reversedType.baseType;
        }

        for (AbstractType<?> n : supportedTypes) {
            if (checkedType.getClass() == n.getClass()) {
                return true;
            }
        }
        return false;
    }

    protected void validate(CFMetaData metadata, String name) {

        ByteBuffer columnName = UTF8Type.instance.decompose(name);

        ColumnDefinition columnDefinition = metadata.getColumnDefinition(columnName);
        if (columnDefinition == null) {
            throw new RuntimeException(String.format("No column definition %s for mapper %s", name, this.name));
        }

        if (columnDefinition.isStatic()) {
            throw new RuntimeException("Lucene indexes are not allowed on static columns as " + name);
        }

        AbstractType<?> type = columnDefinition.type;
        if (!supports(columnDefinition.type)) {
            throw new RuntimeException(String.format("Type '%s' is not supported by mapper '%s'", type, this.name));
        }
    }

    public abstract void validate(CFMetaData metaData);

}
