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
import com.stratio.cassandra.lucene.schema.mapping.{SingleColumnMapper, TextMapper}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.lucene.util.{BytesRefBuilder, NumericUtils, QueryBuilder}

/**
 * A [[Condition]] implementation that matches documents containing a value for a field.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param field the name of the field to be matched
 * @param value the value of the field to be matched
 * @param docValues if the generated query should use doc values
 */
class MatchCondition(val boost : java.lang.Float,
                     val field: String,
                     val value: Any,
                     var docValues: Boolean) extends SingleColumnCondition(boost, field) {

    if (value == null) throw new IndexException("Field value required")
    docValues = if (docValues == null) MatchCondition.DEFAULT_DOC_VALUES else  docValues

    /** @inheritdoc */
    override def doQuery(mapper : SingleColumnMapper[_], analyzer : Analyzer) :  Query = {
        // Check doc values
        if (docValues && !mapper.docValues) throw new IndexException(s"Field '$mapper.field' does not support doc_values")

        val clazz : Class[_]= mapper.base
        var actualQuery : Query = null
        if (clazz == classOf[String]) {
            val base : String = mapper.base(field, value).asInstanceOf[String]
            if (mapper.isInstanceOf[TextMapper]) {
                val queryBuilder : QueryBuilder = new QueryBuilder(analyzer)
                actualQuery = queryBuilder.createPhraseQuery(field, base, 0)
            } else {
                actualQuery = query(base)
            }
            if (actualQuery == null) {
                actualQuery = new BooleanQuery.Builder().build()
            }
        } else if (clazz == classOf[Integer]) {
            actualQuery = query(mapper.base(field, value).asInstanceOf[Integer])
        } else if (clazz == classOf[Long]) {
            actualQuery = query(mapper.base(field, value).asInstanceOf[Long])
        } else if (clazz == classOf[Float]) {
            actualQuery = query(mapper.base(field, value).asInstanceOf[Float])
        } else if (clazz == classOf[Double]) {
            actualQuery = query(mapper.base(field, value).asInstanceOf[Double])
        } else {
            throw new IndexException("Match queries are not supported by mapper '{}'", mapper)
        }
        actualQuery
    }






    def query(value: Any) : Query =
        if (docValues) new DocValuesNumbersQuery(field, docValue(value))
        else  value match {
        case (d:Double) => NumericRangeQuery.newDoubleRange(field, d, d, true, true)
        case (f:Float) => NumericRangeQuery.newFloatRange(field, f, f, true, true)
        case (l:Long) => {
            val ref : BytesRefBuilder = new BytesRefBuilder()
            NumericUtils.longToPrefixCoded(l, 0, ref)
            new TermQuery(new Term(field, ref.toBytesRef))
        }
        case (i: Int) => {
            val ref : BytesRefBuilder = new BytesRefBuilder()
            NumericUtils.intToPrefixCoded(i, 0, ref)
            new TermQuery(new Term(field, ref.toBytesRef))
        }
        case (s:String) => {
            new TermQuery(new Term(field, s))
        }
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper =
        toStringHelper(this).add("value", value).add("docValues", docValues)
}

object MatchCondition {
    /** The default use doc values option. */
    def DEFAULT_DOC_VALUES : Boolean = false
}