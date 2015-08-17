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

import org.apache.cassandra.dht.Token;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Generic query for retrieving a range of tokens in combination with {@link TokenMapperGeneric}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TokenQuery extends MultiTermQuery {

    /** The token mapper. */
    private final TokenMapperGeneric tokenMapper;

    /** The lower accepted token. */
    private final Token lower;

    /** The upper accepted token. */
    private final Token upper;

    /** If the lower token must be included if not null. */
    private final boolean includeLower;

    /** If the upper token must be included if not null. */
    private final boolean includeUpper;

    /**
     * Builds a new {@link TokenQuery}.
     *
     * @param lower        The lower accepted {@link Token}. Maybe null meaning no lower limit.
     * @param upper        The upper accepted {@link Token}. Maybe null meaning no lower limit.
     * @param includeLower If the {@code lowerValue} is included in the range.
     * @param includeUpper If the {@code upperValue} is included in the range.
     * @param tokenMapper  The used {@link TokenMapperGeneric}.
     */
    public TokenQuery(Token lower,
                      Token upper,
                      boolean includeLower,
                      boolean includeUpper,
                      TokenMapperGeneric tokenMapper) {
        super(TokenMapperGeneric.FIELD_NAME);
        this.tokenMapper = tokenMapper;
        this.lower = lower;
        this.upper = upper;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    /** {@inheritDoc} */
    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        TermsEnum termsEnum = terms.iterator();
        return new TokenDataRangeFilteredTermsEnum(termsEnum);
    }

    /** {@inheritDoc} */
    @Override
    public String toString(String field) {
        return new ToStringBuilder(this).append("field", field)
                                        .append("lower", lower)
                                        .append("upper", upper)
                                        .append("includeLower", includeLower)
                                        .append("includeUpper", includeUpper)
                                        .toString();
    }

    /**
     * {@link FilteredTermsEnum} for generic tokens.
     */
    private class TokenDataRangeFilteredTermsEnum extends FilteredTermsEnum {

        /**
         * Builds a new {@link TokenDataRangeFilteredTermsEnum} for the specified {@link TermsEnum}.
         *
         * @param termsEnum The {@link TermsEnum} to be filtered.
         */
        public TokenDataRangeFilteredTermsEnum(TermsEnum termsEnum) {
            super(termsEnum);
            setInitialSeekTerm(new BytesRef());
        }

        /** {@inheritDoc} */
        @Override
        protected AcceptStatus accept(BytesRef term) {
            Token token = tokenMapper.token(term);
            if (includeLower ? token.compareTo(lower) < 0 : token.compareTo(lower) <= 0) {
                return AcceptStatus.NO;
            } else if (includeUpper ? token.compareTo(upper) > 0 : token.compareTo(upper) >= 0) {
                return AcceptStatus.NO;
            } else {
                return AcceptStatus.YES;
            }
        }
    }
}
