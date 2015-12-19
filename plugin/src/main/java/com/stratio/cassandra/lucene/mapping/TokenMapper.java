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

package com.stratio.cassandra.lucene.mapping;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;

/**
 * Class for several token mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class TokenMapper {

    /** The Lucene field name. */

    static final String FIELD_NAME = "_token_murmur";

    /** The Lucene field type. */
    static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setNumericType(FieldType.NumericType.LONG);
        FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
        FIELD_TYPE.freeze();
    }

    public TokenMapper() {
    }

    /**
     * Adds to the specified {@link Document} the {@link Field}s associated to the token of the specified row key.
     *
     * @param document A {@link Document}.
     * @param key The raw partition key to be added.
     */
    public void addFields(Document document, DecoratedKey key) {
        Token token = key.getToken();
        Long value = value(token);
        Field field = new LongField(FIELD_NAME, value, FIELD_TYPE);
        document.add(field);
    }

    private static Long value(Token token) {
        return (Long) token.getTokenValue();
    }
}
