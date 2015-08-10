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
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleColumnMapper<BASE> extends Mapper {

    /**
     * Builds a new {@link SingleColumnMapper} supporting the specified types for indexing and clustering.
     *
     * @param name           The name of the mapper.
     * @param indexed        If the field supports searching.
     * @param sorted         If the field supports sorting.
     * @param supportedTypes The supported Cassandra types for indexing.
     */
    public SingleColumnMapper(String name, Boolean indexed, Boolean sorted, AbstractType... supportedTypes) {
        super(name, indexed, sorted, Arrays.asList(supportedTypes), Collections.singletonList(name));
    }

    /** {@inheritDoc} */
    @Override
    public void addFields(Document document, Columns columns) {
        for (Column column : columns.getColumnsByName(name)) {
            String name = column.getFullName();
            Object value = column.getComposedValue();
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
        BASE base = base(name, value);
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
    public abstract Field indexedField(String name, BASE value);

    /**
     * Returns the {@link Field} to sort by the mapped column.
     *
     * @param name  The name of the column.
     * @param value The value of the column.
     * @return The {@link Field} to sort by the mapped column.
     */
    public abstract Field sortedField(String name, BASE value);

    /**
     * Returns the Lucene type for this mapper.
     *
     * @return The Lucene type for this mapper.
     */
    public abstract Class<BASE> baseClass();

    /**
     * Returns the {@link Column} query value resulting from the mapping of the specified object.
     *
     * @param field The field name.
     * @param value The object to be mapped.
     * @return The {@link Column} index value resulting from the mapping of the specified object.
     */
    public abstract BASE base(String field, Object value);

    /** {@inheritDoc} */
    @Override
    public void validate(CFMetaData metadata) {
        validate(metadata, name);
    }

}
