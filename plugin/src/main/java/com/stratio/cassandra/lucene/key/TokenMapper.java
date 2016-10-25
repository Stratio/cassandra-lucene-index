/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.key;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.util.NumericQueryUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.legacy.LegacyNumericUtils;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import scala.runtime.ByteRef;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Class for several token mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class TokenMapper {

    /** The Lucene field name */
    private static final String FIELD_NAME = "_token";

    /** The Lucene field type */
    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.freeze();
    }

    /**
     * Constructor taking the cache size.
     */
    public TokenMapper() {
        if (!(DatabaseDescriptor.getPartitioner() instanceof Murmur3Partitioner)) {
            throw new IndexException("Only Murmur3 partitioner is supported");
        }
    }

    /**
     * Returns the Lucene {@link IndexableField} associated to the token of the specified row key.
     *
     * @param key the raw partition key to be added
     * @return a list of indexable fields
     */
    public List<IndexableField> indexableFields(DecoratedKey key) {
        Token token = key.getToken();
        Long value = value(token);
        return Arrays.asList(new LongPoint(FIELD_NAME, value), new NumericDocValuesField(FIELD_NAME, value));
    }

    /**
     * Returns the {@link BytesRef} indexing value of the specified Murmur3 partitioning {@link Token}.
     *
     * @param token a Murmur3 token
     * @return the {@code token}'s indexing value
     */
    private static BytesRef bytesRef(Token token) {
        Long value = value(token);
        BytesRefBuilder bytesRef = new BytesRefBuilder();
        byte[] bytes= new byte[8];
        NumericUtils.longToSortableBytes(value, bytes, 0 );
        return new BytesRef(bytes);
    }
    /**
     * Returns the {code Long} value of the specified Murmur3 partitioning {@link Token}.
     *
     * @param token a Murmur3 token
     * @return the {@code token}'s {code Long} value
     */
    public static Long value(Token token) {
        return (Long) token.getTokenValue();
    }

    /**
     * Returns a Lucene {@link SortField} for sorting documents/rows according to the partitioner's order.
     *
     * @return a sort field for sorting by token
     */
    public SortField sortField() {
        return new SortField(FIELD_NAME, SortField.Type.LONG);
    }

    /**
     * Returns if doc values should be used for retrieving token ranges between the specified values.
     *
     * @param start the lower accepted token
     * @param stop the upper accepted token
     * @return {@code true} if doc values should be used, {@code false} other wise
     */
    private static boolean docValues(Long start, Long stop) {
        final long threshold = 1222337203685480000L; // Empirical
        long min = (start == null ? Long.MIN_VALUE : start) / 10;
        long max = (stop == null ? Long.MAX_VALUE : stop) / 10;
        return max - min > threshold;
    }

    /**
     * Returns a Lucene {@link Query} to find the {@link Document}s containing a {@link Token} inside the specified
     * token range.
     *
     * @param lower the lower token
     * @param upper the upper token
     * @param includeLower if the lower token should be included
     * @param includeUpper if the upper token should be included
     * @return the query to find the documents containing a token inside the range
     */
    public Optional<Query> query(Token lower, Token upper, boolean includeLower, boolean includeUpper) {

        // Skip if it's full data range
        if (lower.isMinimum() && upper.isMinimum()) {
            return Optional.empty();
        }

        // Get token values
        Long start = lower.isMinimum() ? null : value(lower);
        Long stop = upper.isMinimum() ? null : value(upper);

        // Do query
        Query query = docValues(start, stop)
                      ? DocValuesRangeQuery.newLongRange(FIELD_NAME, start, stop, includeLower, includeUpper)
                      : NumericQueryUtils.newLongRange(FIELD_NAME, start, stop, includeLower, includeUpper);
        return Optional.of(query);
    }

    /**
     * Returns a Lucene {@link Query} to find the {@link Document}s containing the specified {@link Token}.
     *
     * @param token the token
     * @return the query to find the documents containing {@code token}
     */
    public Query query(Token token) {
        return LongPoint.newExactQuery(FIELD_NAME, value(token));
    }
}
