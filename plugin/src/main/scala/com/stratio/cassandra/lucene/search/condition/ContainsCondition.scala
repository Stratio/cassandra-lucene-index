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
import org.apache.lucene.search.{BooleanClause, BooleanQuery, Query}

/**
 * A [[Condition]] implementation that matches documents containing a value for a field.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
class ContainsCondition(val boost : java.lang.Float,
                        val field: String,
                        val values: Array[Any],
                        var docValues: Boolean) extends SingleColumnCondition(boost, field) {
    if (values == null || values.length == 0) {
        throw new IndexException("Field values required")
    } else {
        docValues = if (docValues == null) ContainsCondition.DEFAULT_DOC_VALUES else docValues
    }

    /** @inheritdoc */
    override def doQuery(mapper : SingleColumnMapper[_], analyzer : Analyzer) : Query = {
        val builder: BooleanQuery.Builder = new BooleanQuery.Builder()
        for (value <- values) {
            val condition : MatchCondition = new MatchCondition(null, field, value, docValues)
            builder.add(condition.doQuery(mapper, analyzer), BooleanClause.Occur.SHOULD)
        }
        builder.build()
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper =
      toStringHelper(this).add("values", values.toString).add("docValues", docValues)

}

object ContainsCondition {
    def DEFAULT_DOC_VALUES : Boolean = false
}