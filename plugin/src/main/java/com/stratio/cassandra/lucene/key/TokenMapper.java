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

package com.stratio.cassandra.lucene.key;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for several token mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class TokenMapper {

    private static final Logger logger = LoggerFactory.getLogger(TokenMapper.class);

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

    public SortField sortField() {
        return new SortField(FIELD_NAME, SortField.Type.LONG);
    }

    /**
     * Returns {@code true} if the specified lower row position kind must be included in the filtered range, {@code
     * false} otherwise.
     *
     * @param position a {@link PartitionPosition}
     * @return {@code true} if the specified lower row position kind must be included in the filtered range, {@code
     * false} otherwise
     */
    public boolean includeStart(PartitionPosition position) {
        logger.debug("START KIND {}", position.kind());
        logger.debug("START INCLUDED {}", position.kind() == PartitionPosition.Kind.MIN_BOUND);
        return position.kind() == PartitionPosition.Kind.MIN_BOUND;
    }

    /**
     * Returns {@code true} if the specified upper row position kind must be included in the filtered range, {@code
     * false} otherwise.
     *
     * @param position a {@link PartitionPosition}
     * @return {@code true} if the specified upper row position kind must be included in the filtered range, {@code
     * false} otherwise
     */
    public boolean includeStop(PartitionPosition position) {
        logger.debug("STOP KIND {}", position.kind());
        logger.debug("STOP INCLUDED {}", position.kind() == PartitionPosition.Kind.MAX_BOUND);
        return position.kind() == PartitionPosition.Kind.MAX_BOUND;
    }

    /**
     * Returns {@code true} if the specified {@link Token} is the minimum accepted, {@code false} otherwise.
     *
     * @param token the {@link Token}
     * @return {@code true} if the specified {@link Token} is the minimum accepted, {@code false} otherwise
     */
    public boolean isMinimum(Token token) {
        Token minimum = DatabaseDescriptor.getPartitioner().getMinimumToken();
        return token.compareTo(minimum) == 0;
    }

    public Query query(Token lower, Token upper, boolean includeLower, boolean includeUpper) {
        Long start = lower == null || lower.isMinimum() ? null : value(lower);
        Long stop = upper == null || upper.isMinimum() ? null : value(upper);
        Query query = DocValuesRangeQuery.newLongRange(FIELD_NAME, start, stop, includeLower, includeUpper);
        return new QueryWrapperFilter(query);
    }

    public Query query(Token token) {
        Long value = value(token);
        Query query = NumericRangeQuery.newLongRange(FIELD_NAME, value, value, true, true);
        return new QueryWrapperFilter(query);
    }
}
