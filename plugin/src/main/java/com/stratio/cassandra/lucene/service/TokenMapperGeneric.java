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

import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 * {@link TokenMapper} to be used when any {@link org.apache.cassandra.dht.IPartitioner} when there is not a more
 * specific implementation. It indexes the token raw binary value as a Lucene string field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TokenMapperGeneric extends TokenMapper {

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_token_generic";

    /** The partitioner token factory. */
    private final TokenFactory factory;

    /** Returns a new {@link TokenMapperGeneric}. */
    public TokenMapperGeneric() {
        super();
        factory = DatabaseDescriptor.getPartitioner().getTokenFactory();
    }

    /** {@inheritDoc} */
    @Override
    public void addFields(Document document, DecoratedKey partitionKey) {
        ByteBuffer bb = factory.toByteArray(partitionKey.getToken());
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        document.add(new StringField(FIELD_NAME, bytesRef, Store.NO));
        document.add(new SortedDocValuesField(FIELD_NAME, bytesRef));
    }

    /** {@inheritDoc} */
    @Override
    protected Query doQuery(Token lower, Token upper, boolean includeLower, boolean includeUpper) {
        return new TokenQuery(lower, upper, includeLower, includeUpper, this);
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Token token) {
        BytesRef ref = bytesRef(token);
        Term term = new Term(FIELD_NAME, ref);
        return new TermQuery(term);
    }

    /** {@inheritDoc} */
    @Override
    public List<SortField> sortFields() {
        return Collections.singletonList(new SortField(FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field, int hits, int sort, boolean reversed)
            throws IOException {
                return new FieldComparator.TermValComparator(hits, field, false) {
                    @Override
                    public int compareValues(BytesRef val1, BytesRef val2) {
                        return token(val1).compareTo(token(val2));
                    }
                };
            }
        }));
    }

    /**
     * Returns the Cassandra {@link Token} represented by the specified Lucene {@link BytesRef}.
     *
     * @param bytesRef A Lucene {@link BytesRef} representation of a Cassandra {@link Token}.
     * @return The Cassandra {@link Token} represented by the specified Lucene {@link BytesRef}.
     */
    Token token(BytesRef bytesRef) {
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        return factory.fromByteArray(bb);
    }

    /**
     * Returns the Lucene {@link BytesRef} represented by the specified Cassandra {@link Token}.
     *
     * @param token A Cassandra {@link Token}.
     * @return The Lucene {@link BytesRef} represented by the specified Cassandra {@link Token}.
     */
    public BytesRef bytesRef(Token token) {
        ByteBuffer bb = factory.toByteArray(token);
        byte[] bytes = ByteBufferUtils.asArray(bb);
        return new BytesRef(bytes);
    }

}
