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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link ColumnMapper} to map a string, not tokenized field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class ColumnMapperKeyword extends ColumnMapperSingle<String> {

    /**
     * @param indexed        If the field supports searching.
     * @param sorted         If the field supports sorting.
     * @param supportedTypes The supported Cassandra types for indexing.
     */
    ColumnMapperKeyword(Boolean indexed, Boolean sorted, AbstractType<?>... supportedTypes) {
        super(indexed, sorted, supportedTypes);
    }

    /** {@inheritDoc} */
    @Override
    public Field indexedField(String name, String value) {
        return new StringField(name, value, STORE);
    }

    /** {@inheritDoc} */
    @Override
    public Field sortedField(String name, String value, boolean isCollection) {
        BytesRef bytes = new BytesRef(value);
        if (isCollection) {
            return new SortedSetDocValuesField(name, bytes);
        } else {
            return new SortedDocValuesField(name, bytes);
        }
    }

    /** {@inheritDoc} */
    @Override
    public final SortField sortField(boolean reverse) {
        return new SortField(name, Type.STRING_VAL, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public final Class<String> baseClass() {
        return String.class;
    }
}
