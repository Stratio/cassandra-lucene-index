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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link Mapper} to map a string, not tokenized field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class KeywordMapper extends SingleColumnMapper.SingleFieldMapper<String> {

    static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.freeze();
    }

    /**
     * Builds  a new {@link KeywordMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param indexed if the field supports searching
     * @param sorted if the field supports sorting
     * @param validated if the field must be validated
     * @param supportedTypes the supported Cassandra types
     */
    KeywordMapper(String field,
                  String column,
                  Boolean indexed,
                  Boolean sorted,
                  Boolean validated,
                  AbstractType<?>... supportedTypes) {
        super(field, column, indexed, sorted, validated, KEYWORD_ANALYZER, String.class, supportedTypes);
    }

    /** {@inheritDoc} */
    @Override
    public Field indexedField(String name, String value) {
        return new Field(name, value, FIELD_TYPE);
    }

    /** {@inheritDoc} */
    @Override
    public Field sortedField(String name, String value) {
        return new SortedDocValuesField(name, new BytesRef(value));
    }

    /** {@inheritDoc} */
    @Override
    public final SortField sortField(String name, boolean reverse) {
        return new SortField(name, Type.STRING_VAL, reverse);
    }
}
