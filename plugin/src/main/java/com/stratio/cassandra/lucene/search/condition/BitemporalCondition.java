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

package com.stratio.cassandra.lucene.search.condition;

import static com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper.BitemporalDateTime;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;

import com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper;

/**
 * A {@link Condition} implementation that matches bi-temporal (four) fields within two range of values.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalCondition extends SingleMapperCondition<BitemporalMapper> {

    /** The default from value for vtFrom and ttFrom. */
    public static final Long DEFAULT_FROM = 0L;

    /** The default to value for vtTo and ttTo. */
    public static final Long DEFAULT_TO = Long.MAX_VALUE;

    /** The Valid Time Start. */
    public final Object vtFrom;

    /** The Valid Time End. */
    public final Object vtTo;

    /** The Transaction Time Start. */
    public final Object ttFrom;

    /** The Transaction Time End. */
    public final Object ttTo;

    /**
     * Constructs a query selecting all fields that intersects with valid time and transaction time ranges including
     * limits.
     *
     * @param boost     The boost for this query clause. Documents matching this clause will (in addition to the normal
     *                  weightings) have their score multiplied by {@code boost}.
     * @param field     The name of the field to be matched.
     * @param vtFrom    The Valid Time Start.
     * @param vtTo      The Valid Time End.
     * @param ttFrom    The Transaction Time Start.
     * @param ttTo      The Transaction Time End.
     *
     */
    public BitemporalCondition(Float boost,
                               String field,
                               Object vtFrom,
                               Object vtTo,
                               Object ttFrom,
                               Object ttTo) {
        super(boost, field, BitemporalMapper.class);
        this.vtFrom = vtFrom;
        this.vtTo = vtTo;
        this.ttFrom = ttFrom;
        this.ttTo = ttTo;
    }

    @Override
    public Query query(BitemporalMapper mapper, Analyzer analyzer) {

        Long vt_from = this.vtFrom == null ?
                new BitemporalDateTime(DEFAULT_FROM).toDate().getTime() :
                mapper.parseBitemporalDate(this.vtFrom).toDate().getTime();
        Long vt_to = this.vtTo == null ?
                new BitemporalDateTime(DEFAULT_TO).toDate().getTime() :
                mapper.parseBitemporalDate(this.vtTo).toDate().getTime();
        Long tt_from = this.ttFrom == null ?
                new BitemporalDateTime(DEFAULT_FROM).toDate().getTime() :
                mapper.parseBitemporalDate(this.ttFrom).toDate().getTime();
        Long tt_to = this.ttTo == null ?
                new BitemporalDateTime(DEFAULT_TO).toDate().getTime() :
                mapper.parseBitemporalDate(this.ttTo).toDate().getTime();

        Long MIN=BitemporalDateTime.MIN.toDate().getTime();
        Long MAX=BitemporalDateTime.MAX.toDate().getTime();

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        BooleanQuery.Builder validBuilder= new BooleanQuery.Builder();
        validBuilder.add(NumericRangeQuery.newLongRange(field + ".vtFrom", vt_from, vt_to, true, true), SHOULD);
        validBuilder.add(NumericRangeQuery.newLongRange(field + ".vtTo", vt_from, vt_to, true, true),SHOULD);

        BooleanQuery.Builder containsValidBuilder= new BooleanQuery.Builder();
        containsValidBuilder.add(NumericRangeQuery.newLongRange(field+".vtFrom",MIN,vt_from,true,true),MUST);
        containsValidBuilder.add(NumericRangeQuery.newLongRange(field+".vtTo ",vt_to,MAX,true,true),MUST);
        validBuilder.add(containsValidBuilder.build(),SHOULD);


        BooleanQuery.Builder transactionBuilder= new BooleanQuery.Builder();
        transactionBuilder.add(NumericRangeQuery.newLongRange(field + ".ttFrom", tt_from, tt_to, true, true), SHOULD);
        transactionBuilder.add(NumericRangeQuery.newLongRange(field + ".ttTo", tt_from, tt_to, true, true),SHOULD);

        BooleanQuery.Builder containsTransactionBuilder= new BooleanQuery.Builder();
        containsTransactionBuilder.add(NumericRangeQuery.newLongRange(field+".ttFrom",MIN,tt_from,true,true),MUST);
        containsTransactionBuilder.add(NumericRangeQuery.newLongRange(field+".ttTo ",tt_to,MAX,true,true),MUST);
        transactionBuilder.add(containsTransactionBuilder.build(),SHOULD);

        builder.add(validBuilder.build(),MUST);
        builder.add(transactionBuilder.build(),MUST);

        Query query = builder.build();
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("vtFrom", vtFrom)
                                   .add("vtTo", vtTo)
                                   .add("ttFrom", ttFrom)
                                   .add("ttTo", ttTo)
                                   .toString();
    }
}
