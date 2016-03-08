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

import com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import static com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper.BitemporalDateTime;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;
import static org.apache.lucene.search.NumericRangeQuery.newLongRange;

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
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field The name of the field to be matched
     * @param vtFrom the valid time start
     * @param vtTo the valid time end
     * @param ttFrom the transaction time start
     * @param ttTo the transaction time end
     */
    public BitemporalCondition(Float boost, String field, Object vtFrom, Object vtTo, Object ttFrom, Object ttTo) {
        super(boost, field, BitemporalMapper.class);
        this.vtFrom = vtFrom;
        this.vtTo = vtTo;
        this.ttFrom = ttFrom;
        this.ttTo = ttTo;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(BitemporalMapper mapper, Analyzer analyzer) {

        Long vtFromTime = parseTime(mapper, DEFAULT_FROM, vtFrom);
        Long vtToTime = parseTime(mapper, DEFAULT_TO, vtTo);
        Long ttFromTime = parseTime(mapper, DEFAULT_FROM, ttFrom);
        Long ttToTime = parseTime(mapper, DEFAULT_TO, ttTo);

        Long minTime = BitemporalDateTime.MIN.toDate().getTime();
        Long maxTime = BitemporalDateTime.MAX.toDate().getTime();

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        if (!((vtFromTime.equals(0L)) && (vtToTime.equals(Long.MAX_VALUE)))) {

            BooleanQuery.Builder validBuilder = new BooleanQuery.Builder();
            validBuilder.add(newLongRange(field + BitemporalMapper.VT_FROM_FIELD_SUFFIX,
                                          vtFromTime,
                                          vtToTime,
                                          true,
                                          true), SHOULD);
            validBuilder.add(newLongRange(field + BitemporalMapper.VT_TO_FIELD_SUFFIX,
                                          vtFromTime,
                                          vtToTime,
                                          true,
                                          true), SHOULD);

            BooleanQuery.Builder containsValidBuilder = new BooleanQuery.Builder();
            containsValidBuilder.add(newLongRange(field + BitemporalMapper.VT_FROM_FIELD_SUFFIX,
                                                  minTime,
                                                  vtFromTime,
                                                  true,
                                                  true), MUST);
            containsValidBuilder.add(newLongRange(field + BitemporalMapper.VT_TO_FIELD_SUFFIX,
                                                  vtToTime,
                                                  maxTime,
                                                  true,
                                                  true), MUST);
            validBuilder.add(containsValidBuilder.build(), SHOULD);
            builder.add(validBuilder.build(), MUST);
        }

        if (!((ttFromTime.equals(0L)) && (ttToTime.equals(Long.MAX_VALUE)))) {

            BooleanQuery.Builder transactionBuilder = new BooleanQuery.Builder();
            transactionBuilder.add(newLongRange(field + BitemporalMapper.TT_FROM_FIELD_SUFFIX,
                                                ttFromTime,
                                                ttToTime,
                                                true,
                                                true), SHOULD);
            transactionBuilder.add(newLongRange(field + BitemporalMapper.TT_TO_FIELD_SUFFIX,
                                                ttFromTime,
                                                ttToTime,
                                                true,
                                                true), SHOULD);

            BooleanQuery.Builder containsTransactionBuilder = new BooleanQuery.Builder();
            containsTransactionBuilder.add(newLongRange(field + BitemporalMapper.TT_FROM_FIELD_SUFFIX,
                                                        minTime,
                                                        ttFromTime,
                                                        true,
                                                        true), MUST);
            containsTransactionBuilder.add(newLongRange(field + BitemporalMapper.TT_TO_FIELD_SUFFIX,
                                                        ttToTime,
                                                        maxTime,
                                                        true,
                                                        true), MUST);
            transactionBuilder.add(containsTransactionBuilder.build(), SHOULD);
            builder.add(transactionBuilder.build(), MUST);
        }

        Query query = builder.build();
        query.setBoost(boost);
        return query;
    }

    private static Long parseTime(BitemporalMapper mapper, Long defaultTime, Object value) {
        return value == null
               ? new BitemporalDateTime(defaultTime).toDate().getTime()
               : mapper.parseBitemporalDate(value).toDate().getTime();
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
