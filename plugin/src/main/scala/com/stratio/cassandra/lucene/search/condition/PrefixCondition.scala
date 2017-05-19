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

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.Term
import org.apache.lucene.search.PrefixQuery
import org.apache.lucene.search.Query

/**
 * A [[Condition]] implementation that matches documents containing terms with a specified prefix.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param field the name of the field to be matched
 * @param value the field prefix to be matched
 */
class PrefixCondition(val boost : java.lang.Float,
    @JsonProperty("field") val field: String,
    @JsonProperty("value") val value: String) extends SingleColumnCondition(boost, field) {

    if (value == null) throw new IndexException("Field value required")

    /** @inheritdoc */
    override def doQuery(mapper : SingleColumnMapper[_],  analyzer : Analyzer) :  Query = {
        if (mapper.base == classOf[String]) {
            val term : Term = new Term(field, value)
            new PrefixQuery(term)
        } else {
            throw new IndexException(s"Prefix queries are not supported by mapper '$mapper'")
        }
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper =
        toStringHelper(this).add("value", value)

}