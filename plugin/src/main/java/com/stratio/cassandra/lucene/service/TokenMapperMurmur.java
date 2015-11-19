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

package com.stratio.cassandra.lucene.service;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.DocValuesRangeQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

import java.util.Collections;
import java.util.List;

/**
 * {@link PartitionKeyMapper} to be used when {@link org.apache.cassandra.dht.Murmur3Partitioner} is used. It indexes
 * the token long value as a Lucene long field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TokenMapperMurmur extends TokenMapper {

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

    /**
     * Builds a new {@link TokenMapperMurmur}.
     */
    public TokenMapperMurmur() {
        super();
    }

    /** {@inheritDoc} */
    @Override
    public void addFields(Document document, DecoratedKey partitionKey) {
        Token token = partitionKey.getToken();
        Long value = value(token);
        document.add(new LongField(FIELD_NAME, value, FIELD_TYPE));
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Token token) {
        Long value = value(token);
        return NumericRangeQuery.newLongRange(FIELD_NAME, value, value, true, true);
    }

    /** {@inheritDoc} */
    @Override
    protected Query doQuery(Token lower, Token upper, boolean includeLower, boolean includeUpper) {
        Long start = lower == null || lower.isMinimum() ? null : value(lower);
        Long stop = upper == null || upper.isMinimum() ? null : value(upper);
        return DocValuesRangeQuery.newLongRange(FIELD_NAME, start, stop, includeLower, includeUpper);
    }

    /** {@inheritDoc} */
    @Override
    public List<SortField> sortFields() {
        return Collections.singletonList(new SortField(FIELD_NAME, SortField.Type.LONG));
    }

    static Long value(Token token) {
        return (Long) token.getTokenValue();
    }

}
