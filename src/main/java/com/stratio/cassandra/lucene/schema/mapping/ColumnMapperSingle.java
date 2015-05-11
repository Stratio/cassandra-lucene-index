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

import com.stratio.cassandra.lucene.schema.Column;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class ColumnMapperSingle<BASE> extends ColumnMapper {

    /**
     * Builds a new {@link ColumnMapperSingle} supporting the specified types for indexing and clustering.
     *
     * @param indexed        If the field supports searching.
     * @param sorted         If the field supports sorting.
     * @param supportedTypes The supported Cassandra types for indexing.
     */
    ColumnMapperSingle(Boolean indexed, Boolean sorted, AbstractType<?>... supportedTypes) {
        super(indexed, sorted, supportedTypes);
    }

    @Override
    public final void addFields(Document document, String name, Object value, boolean isCollection) {
        BASE base = base(name, value);
        if (indexed) document.add(indexedField(name, base));
        if (sorted) document.add(sortedField(name, base, isCollection));
    }

    public abstract Field indexedField(String name, BASE value);

    public abstract Field sortedField(String name, BASE value, boolean isCollection);

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

}
