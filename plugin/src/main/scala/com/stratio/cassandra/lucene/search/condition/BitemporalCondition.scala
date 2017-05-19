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
package com.stratio.cassandra.lucene.search.condition

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper
import com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper.BitemporalDateTime
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.BooleanClause.Occur.{MUST, SHOULD}
import org.apache.lucene.search.NumericRangeQuery.newLongRange
import org.apache.lucene.search.{BooleanQuery, Query}

import scala.Long.MaxValue

/**
 * A [[Condition]] implementation that matches bi-temporal (four) fields within two range of values.
 *
 * @author Eduardo Alonso `eduardoalonso@stratio.com`
  * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
  * weightings) have their score multiplied by {{{boost}.
  * @param field the name of the field to be matched
  * @param vtFrom the valid time start
  * @param vtTo the valid time end
  * @param ttFrom the transaction time start
  * @param ttTo the transaction time end
 */
class BitemporalCondition(val boost : java.lang.Float, val field : String, val vtFrom : Any, val vtTo : Any, val ttFrom : Any, val ttTo : Any) extends SingleMapperCondition[BitemporalMapper](boost,field, classOf[BitemporalMapper]) {

    /** @inheritdoc */
    override def doQuery(mapper : BitemporalMapper, analyzer : Analyzer ) :  Query = {

        val vtFromTime : Long = BitemporalCondition.parseTime(mapper, BitemporalCondition.DEFAULT_FROM, vtFrom)
        val vtToTime : Long = BitemporalCondition.parseTime(mapper, BitemporalCondition.DEFAULT_TO, vtTo)
        val ttFromTime : Long = BitemporalCondition.parseTime(mapper, BitemporalCondition.DEFAULT_FROM, ttFrom)
        val ttToTime: Long = BitemporalCondition.parseTime(mapper, BitemporalCondition.DEFAULT_TO, ttTo)

        val minTime : Long = BitemporalDateTime.MIN.toTimestamp()
        val maxTime : Long = BitemporalDateTime.MAX.toTimestamp()

        val builder : BooleanQuery.Builder  = new BooleanQuery.Builder()

        if (!(vtFromTime.equals(0L) && vtToTime.equals(MaxValue))) {

            val validBuilder : BooleanQuery.Builder = new BooleanQuery.Builder()
            validBuilder.add(newLongRange(field + BitemporalMapper.VT_FROM_FIELD_SUFFIX,
                                          vtFromTime,
                                          vtToTime,
                                          true,
                                          true), SHOULD)
            validBuilder.add(newLongRange(field + BitemporalMapper.VT_TO_FIELD_SUFFIX,
                                          vtFromTime,
                                          vtToTime,
                                          true,
                                          true), SHOULD)

            val containsValidBuilder : BooleanQuery.Builder= new BooleanQuery.Builder()
            containsValidBuilder.add(newLongRange(field + BitemporalMapper.VT_FROM_FIELD_SUFFIX,
                                                  minTime,
                                                  vtFromTime,
                                                  true,
                                                  true), MUST)
            containsValidBuilder.add(newLongRange(field + BitemporalMapper.VT_TO_FIELD_SUFFIX,
                                                  vtToTime,
                                                  maxTime,
                                                  true,
                                                  true), MUST)
            validBuilder.add(containsValidBuilder.build(), SHOULD)
            builder.add(validBuilder.build(), MUST)
        }

        if (!(ttFromTime.equals(0L) && ttToTime.equals(MaxValue))) {

            val transactionBuilder : BooleanQuery.Builder = new BooleanQuery.Builder()
            transactionBuilder.add(newLongRange(field + BitemporalMapper.TT_FROM_FIELD_SUFFIX,
                                                ttFromTime,
                                                ttToTime,
                                                true,
                                                true), SHOULD)
            transactionBuilder.add(newLongRange(field + BitemporalMapper.TT_TO_FIELD_SUFFIX,
                                                ttFromTime,
                                                ttToTime,
                                                true,
                                                true), SHOULD)

            val containsTransactionBuilder : BooleanQuery.Builder  = new BooleanQuery.Builder()
            containsTransactionBuilder.add(newLongRange(field + BitemporalMapper.TT_FROM_FIELD_SUFFIX,
                                                        minTime,
                                                        ttFromTime,
                                                        true,
                                                        true), MUST)
            containsTransactionBuilder.add(newLongRange(field + BitemporalMapper.TT_TO_FIELD_SUFFIX,
                                                        ttToTime,
                                                        maxTime,
                                                        true,
                                                        true), MUST)
            transactionBuilder.add(containsTransactionBuilder.build(), SHOULD)
            builder.add(transactionBuilder.build(), MUST)
        }

        builder.build()
    }


    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper = {
        toStringHelper(this).add("vtFrom", vtFrom)
                                   .add("vtTo", vtTo)
                                   .add("ttFrom", ttFrom)
                                   .add("ttTo", ttTo)
    }
}

object BitemporalCondition {

  /** The default from value for vtFrom and ttFrom. */
  val DEFAULT_FROM : Long = 0L

  /** The default to value for vtTo and ttTo. */
  val DEFAULT_TO : Long = MaxValue

  def parseTime(mapper: BitemporalMapper, defaultTime : Long, value : Any) : Long =
    if (value == null) new BitemporalDateTime(defaultTime).toTimestamp() else mapper.parseBitemporalDate(value).toTimestamp()
}