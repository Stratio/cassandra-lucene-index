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
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.Query
import org.apache.lucene.util.QueryBuilder

/**
 * A [[Condition]] implementation that matches documents containing a particular sequence of terms.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param field the name of the field to be matched
 * @param value the phrase terms to be matched
 * @param slop the number of other words permitted between words in phrase
 */
class PhraseCondition(  val boost : java.lang.Float,
                        val field: String,
                        val value: String,
                        var slop: Int) extends SingleColumnCondition(boost, field) {
    if (value == null) {
        throw new IndexException("Field value required")
    } else if (slop != null && slop < 0) {
        throw new IndexException("Slop must be positive")
    } else {
        slop = if (slop == null) PhraseCondition.DEFAULT_SLOP else slop
    }


    /** @inheritdoc */
    override def doQuery(mapper : SingleColumnMapper[_], analyzer : Analyzer) : Query = {
        if (mapper.base == classOf[String]) {
            val queryBuilder : QueryBuilder  = new QueryBuilder(analyzer)
            var query : Query = queryBuilder.createPhraseQuery(field, value, slop)
            if (query == null) {
                query = new BooleanQuery.Builder().build()
            }
            query
        } else {
            throw new IndexException(s"Phrase queries are not supported by mapper '$mapper'")
        }
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper =
        toStringHelper(this).add("value", value).add("slop", slop);

}

object PhraseCondition {
    /** The default umber of other words permitted between words in phrase. */
    def DEFAULT_SLOP : Int = 0
}