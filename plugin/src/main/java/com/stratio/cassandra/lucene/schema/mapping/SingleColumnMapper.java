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

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.util.Arrays;
import java.util.Collections;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @param <T> THe base type.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleColumnMapper<T> extends Mapper {

    protected final String column;

    /**
     * Builds a new {@link SingleColumnMapper} supporting the specified types for indexing and clustering.
     *
     * @param name           The name of the mapper.
     * @param column         The name of the column to be mapped.
     * @param indexed        If the field supports searching.
     * @param sorted         If the field supports sorting.
     * @param supportedTypes The supported Cassandra types for indexing.
     */
    public SingleColumnMapper(String name,
                              String column,
                              Boolean indexed,
                              Boolean sorted,
                              AbstractType<?>... supportedTypes) {
        super(name,
              indexed,
              sorted,
              Arrays.asList(supportedTypes),
              Collections.singletonList(column == null ? name : column));
        this.column = column == null ? name : column;
    }

    /** {@inheritDoc} */
    @Override
    public void addFields(Document document, Columns columns) {
        for (Column<?> c : columns.getColumnsByName(column)) {
            String name = c.getFullName(this.name);
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
        T base = base(name, value);
        if (indexed) {
            document.add(indexedField(name, base));
        }
        if (sorted) {
            document.add(sortedField(name, base));
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
     * Returns the Lucene type for this mapper.
     *
     * @return The Lucene type for this mapper.
     */
    public abstract Class<T> baseClass();

    /**
     * Returns the {@link Column} query value resulting from the mapping of the specified object.
     *
     * @param field The field name.
     * @param value The object to be mapped.
     * @return The {@link Column} index value resulting from the mapping of the specified object.
     */
    public abstract T base(String field, Object value);

    /** {@inheritDoc} */
    @Override
    public void validate(CFMetaData metadata) {
        validate(metadata, column);
    }

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
