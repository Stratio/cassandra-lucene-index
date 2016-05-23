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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Class for several row partitioning {@link Token} mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class TokenMapper {

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_token";

    /** The Lucene field type. */
    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setNumericType(FieldType.NumericType.LONG);
        FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
        FIELD_TYPE.freeze();
    }

    /** Default constructor. */
    public TokenMapper() {
        if (!(DatabaseDescriptor.getPartitioner() instanceof Murmur3Partitioner)) {
            throw new IndexException("Only Murmur3 partitioner is supported");
        }
    }

    /**
     * @param token A {@link Token}.
     * @return the {@code token}'s {@link Long} value
     */
    public static Long value(Token token) {
        return (Long) token.getTokenValue();
    }

    /**
     * Adds to the specified {@link Document} the {@link org.apache.lucene.document.Field}s associated to the token of
     * the specified row key.
     *
     * @param document A {@link Document}.
     * @param partitionKey The raw partition key to be added.
     */
    public void addFields(Document document, DecoratedKey partitionKey) {
        Token token = partitionKey.getToken();
        Long value = value(token);
        document.add(new LongField(FIELD_NAME, value, FIELD_TYPE));
    }

    /**
     * Returns a Lucene {@link Query} for retrieving the documents inside the specified {@link Token} range.
     *
     * @param lower The lower accepted {@link Token}. Maybe null meaning no lower limit.
     * @param upper The upper accepted {@link Token}. Maybe null meaning no lower limit.
     * @param includeLower If the {@code lowerValue} is included in the range.
     * @param includeUpper If the {@code upperValue} is included in the range.
     * @return A Lucene {@link Query} for retrieving the documents inside the specified {@link Token} range.
     */
    public Query query(Token lower, Token upper, boolean includeLower, boolean includeUpper) {
        if (lower != null && upper != null) {
            if (isMinimum(lower) && isMinimum(upper) && (includeLower || includeUpper)) {
                return null;
            }
        }
        return doQuery(lower, upper, includeLower, includeUpper);
    }

    /**
     * Returns {@code true} if the specified {@link Token} is the minimum accepted by the {@link IPartitioner}, {@code
     * false} otherwise.
     *
     * @param token A {@link Token}.
     * @return {@code true} if the specified {@link Token} is the minimum accepted by the {@link IPartitioner}, {@code
     * false} otherwise.
     */
    public boolean isMinimum(Token token) {
        Token minimum = DatabaseDescriptor.getPartitioner().getMinimumToken();
        return token.compareTo(minimum) == 0;
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
        NumericUtils.longToPrefixCoded(value, 0, bytesRef);
        return bytesRef.get();
    }

    /**
     * Returns a Lucene {@link Query} for retrieving the documents with the specified {@link Token}.
     *
     * @param token A {@link Token}.
     * @return A Lucene {@link Query} for retrieving the documents with the specified {@link Token}.
     */
    public Query query(Token token) {
        return new TermQuery(new Term(FIELD_NAME, bytesRef(token)));
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
     * Returns a Lucene {@link Query} for retrieving the documents inside the specified {@link Token} range.
     *
     * @param lower The lower accepted {@link Token}. Maybe null meaning no lower limit.
     * @param upper The upper accepted {@link Token}. Maybe null meaning no lower limit.
     * @param includeLower If the {@code lowerValue} is included in the range.
     * @param includeUpper If the {@code upperValue} is included in the range.
     * @return A Lucene {@link Query} for retrieving the documents inside the specified {@link Token} range.
     */
    private Query doQuery(Token lower, Token upper, boolean includeLower, boolean includeUpper) {
        Long start = lower == null || lower.isMinimum() ? null : value(lower);
        Long stop = upper == null || upper.isMinimum() ? null : value(upper);
        return docValues(start, stop)
               ? DocValuesRangeQuery.newLongRange(FIELD_NAME, start, stop, includeLower, includeUpper)
               : NumericRangeQuery.newLongRange(FIELD_NAME, start, stop, includeLower, includeUpper);
    }

    /**
     * Returns a Lucene {@link SortField} list for sorting documents/rows according to the current partitioner.
     *
     * @return A Lucene {@link SortField} list for sorting documents/rows according to the current partitioner.
     */
    public SortField sortField() {
        return new SortField(FIELD_NAME, SortField.Type.LONG);
    }

    /**
     * Returns {@code true} if the specified lower row position kind must be included in the filtered range, {@code
     * false} otherwise.
     *
     * @param rowPosition A {@link RowPosition}.
     * @return {@code true} if the specified lower row position kind must be included in the filtered range, {@code
     * false} otherwise.
     */
    public boolean includeStart(RowPosition rowPosition) {
        return rowPosition.kind() != RowPosition.Kind.MAX_BOUND;
    }

    /**
     * Returns {@code true} if the specified upper row position kind must be included in the filtered range, {@code
     * false} otherwise.
     *
     * @param rowPosition A {@link RowPosition}.
     * @return {@code true} if the specified upper row position kind must be included in the filtered range, {@code
     * false} otherwise.
     */
    public boolean includeStop(RowPosition rowPosition) {
        return rowPosition.kind() != RowPosition.Kind.MIN_BOUND;
    }

    /**
     * Returns a token based {@link Row} {@link Comparator}.
     *
     * @return A token based {@link Row} {@link Comparator}.
     */
    public Comparator<Row> comparator() {
        return new Comparator<Row>() {
            @Override
            public int compare(Row row1, Row row2) {
                Token t1 = row1.key.getToken();
                Token t2 = row2.key.getToken();
                return t1.compareTo(t2);
            }
        };
    }

    private static final BigInteger OFFSET = BigInteger.valueOf(Long.MIN_VALUE).negate();

    /**
     * Returns a lexicographically sortable representation of the specified token.
     *
     * @param token the token
     * @return a UTF-8 string serialized as a byte buffer
     */
    static ByteBuffer toCollated(Token token) {
        long value = value(token);
        BigInteger afterOffset = BigInteger.valueOf(value).add(OFFSET);
        String text = String.format("%016x", afterOffset);
        return UTF8Type.instance.decompose(text);
    }

    /**
     * Returns the token represented by the specified output of {@link #toCollated(Token)}.
     *
     * @param bb a byte buffer generated with {@link #toCollated(Token)}
     * @return the token represented by {@code bb}
     */
    static Token fromCollated(ByteBuffer bb) {
        String text = UTF8Type.instance.compose(bb);
        BigInteger beforeOffset = new BigInteger(text, 16);
        long value = beforeOffset.subtract(OFFSET).longValue();
        return new Murmur3Partitioner.LongToken(value);
    }
}
