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

import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@link TokenMapper} to be used when any {@link IPartitioner} when there is not a more specific implementation. It
 * indexes the token raw binary value as a Lucene string field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class TokenMapperGeneric extends TokenMapper {

    public static final String FIELD_NAME = "_token_generic"; // The Lucene field name

    private final TokenFactory factory; // The partitioner token factory

    /** Returns a new {@link TokenMapperGeneric}. */
    public TokenMapperGeneric(CFMetaData metadata) {
        super(metadata);
        factory = DatabaseDescriptor.getPartitioner().getTokenFactory();
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public void addFields(Document document, DecoratedKey partitionKey) {
        ByteBuffer bb = factory.toByteArray(partitionKey.getToken());
        String serialized = ByteBufferUtils.toString(bb);
        BytesRef bytesRef = new BytesRef(serialized);
        document.add(new StringField(FIELD_NAME, serialized, Store.NO));
        document.add(new SortedDocValuesField(FIELD_NAME, bytesRef));
    }

    /** {@inheritDoc} */
    @Override
    protected Query makeQuery(Token lower, Token upper, boolean includeLower, boolean includeUpper) {
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
    public SortField[] sortFields() {
        return new SortField[]{new SortField(FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field,
                                                    int hits,
                                                    int sort,
                                                    boolean reversed) throws IOException {
                return new FieldComparator.TermOrdValComparator(hits, field, false) {
                    @Override
                    public int compareValues(BytesRef val1, BytesRef val2) {
                        return token(val1).compareTo(token(val2));
                    }
                };
            }
        })};
    }

    /**
     * Returns the Cassandra {@link Token} represented by the specified Lucene {@link BytesRef}.
     *
     * @param bytesRef A Lucene {@link BytesRef} representation of a Cassandra {@link Token}.
     * @return The Cassandra {@link Token} represented by the specified Lucene {@link BytesRef}.
     */
    Token token(BytesRef bytesRef) {
        String string = bytesRef.utf8ToString();
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return factory.fromByteArray(bb);
    }

    /**
     * Returns the Lucene {@link BytesRef} represented by the specified Cassandra {@link Token}.
     *
     * @param token A Cassandra {@link Token}.
     * @return The Lucene {@link BytesRef} represented by the specified Cassandra {@link Token}.
     */
    @SuppressWarnings("unchecked")
    public BytesRef bytesRef(Token token) {
        ByteBuffer bb = factory.toByteArray(token);
        byte[] bytes = ByteBufferUtils.asArray(bb);
        return new BytesRef(bytes);
    }

}
