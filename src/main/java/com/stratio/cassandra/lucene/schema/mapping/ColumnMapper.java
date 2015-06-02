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

import com.stratio.cassandra.lucene.geospatial.GeoShapeMapper;
import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.search.SortField;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = ColumnMapperBlob.class, name = "bytes"),
               @JsonSubTypes.Type(value = ColumnMapperBoolean.class, name = "boolean"),
               @JsonSubTypes.Type(value = ColumnMapperDate.class, name = "date"),
               @JsonSubTypes.Type(value = ColumnMapperDouble.class, name = "double"),
               @JsonSubTypes.Type(value = ColumnMapperFloat.class, name = "float"),
               @JsonSubTypes.Type(value = ColumnMapperInet.class, name = "inet"),
               @JsonSubTypes.Type(value = ColumnMapperInteger.class, name = "integer"),
               @JsonSubTypes.Type(value = ColumnMapperLong.class, name = "long"),
               @JsonSubTypes.Type(value = ColumnMapperString.class, name = "string"),
               @JsonSubTypes.Type(value = ColumnMapperText.class, name = "text"),
               @JsonSubTypes.Type(value = ColumnMapperUUID.class, name = "uuid"),
               @JsonSubTypes.Type(value = ColumnMapperBigDecimal.class, name = "bigdec"),
               @JsonSubTypes.Type(value = ColumnMapperBigInteger.class, name = "bigint"),
               @JsonSubTypes.Type(value = GeoShapeMapper.class, name = "geo_shape"),})
public abstract class ColumnMapper {

    /** A no-action getAnalyzer for not tokenized {@link ColumnMapper} implementations. */
    static final String KEYWORD_ANALYZER = PreBuiltAnalyzers.KEYWORD.toString();

    /** The store field in Lucene default option. */
    static final Store STORE = Store.NO;

    /** If the field must be indexed when no specified. */
    static final boolean DEFAULT_INDEXED = true;

    /** If the field must be sorted when no specified. */
    static final boolean DEFAULT_SORTED = true;

    protected String name;

    /** If the field must be indexed. */
    final Boolean indexed;

    /** If the field must be sorted. */
    final Boolean sorted;

    /** The supported Cassandra types for indexing. */
    private final AbstractType<?>[] supportedTypes;

    /**
     * Builds a new {@link ColumnMapper} supporting the specified types for indexing.
     *
     * @param indexed        If the field supports searching.
     * @param sorted         If the field supports sorting.
     * @param supportedTypes The supported Cassandra types for indexing.
     */
    protected ColumnMapper(Boolean indexed, Boolean sorted, AbstractType<?>... supportedTypes) {
        this.indexed = indexed == null ? DEFAULT_INDEXED : indexed;
        this.sorted = sorted == null ? DEFAULT_SORTED : sorted;
        this.supportedTypes = supportedTypes;
    }

    public void init(String name) {
        this.name = name;
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
     * @param columns   The {@link Columns}.
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
    public boolean supports(final AbstractType<?> type) {
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

}
