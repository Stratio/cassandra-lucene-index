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
package com.stratio.cassandra.lucene.service;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

/**
 * {@link PartitionKeyMapper} to be used when {@link org.apache.cassandra.dht.Murmur3Partitioner} is used. It indexes
 * the token long value as a Lucene long field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class TokenMapperMurmur extends TokenMapper {

    private static final String FIELD_NAME = "_token_murmur"; // The Lucene field name
    private static final FieldType FIELD_TYPE = new FieldType();

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
        Long value = (Long) partitionKey.getToken().getTokenValue();
        document.add(new LongField(FIELD_NAME, value, FIELD_TYPE));
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Token token) {
        Long value = (Long) token.getTokenValue();
        return NumericRangeQuery.newLongRange(FIELD_NAME, value, value, true, true);
    }

    /** {@inheritDoc} */
    @Override
    protected Query makeQuery(Token lower, Token upper, boolean includeLower, boolean includeUpper) {
        Long start = lower == null ? null : (Long) lower.getTokenValue();
        Long stop = upper == null ? null : (Long) upper.getTokenValue();
        if (lower != null && lower.isMinimum()) {
            start = null;
        }
        if (upper != null && upper.isMinimum()) {
            stop = null;
        }
        if (start == null && stop == null) {
            return null;
        }
        return NumericRangeQuery.newLongRange(FIELD_NAME, start, stop, includeLower, includeUpper);
    }

    /** {@inheritDoc} */
    @Override
    public SortField[] sortFields() {
        return new SortField[]{new SortField(FIELD_NAME, SortField.Type.LONG)};
    }

}
