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
import org.apache.lucene.search._

/**
 * A [[Condition]] implementation that matches a field within an range of values.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
  * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param field the name of the field to be matched.
 * @param lower the lower accepted value. Maybe {{{null} meaning no lower limit
 * @param upper the upper accepted value. Maybe {{{null} meaning no upper limit
 * @param includeLower if {{{true}, the {{{lowerValue} is included in the range
 * @param includeUpper if {{{true}, the {{{upperValue} is included in the range
 * @param docValues if the generated query should use doc values
 */
class RangeCondition(   val boost : java.lang.Float,
                        val field: String,
                        val lower: Any,
                        val upper: Any,
                        var includeLower: Boolean,
                        var includeUpper: Boolean,
                        var docValues: Boolean) extends SingleColumnCondition(boost, field) {

    includeLower = if (includeLower == null) RangeCondition.DEFAULT_INCLUDE_LOWER else includeLower
    includeUpper = if (includeUpper == null) RangeCondition.DEFAULT_INCLUDE_UPPER else includeUpper
    docValues = if (docValues == null) RangeCondition.DEFAULT_DOC_VALUES else docValues

    /** @inheritdoc */
    override def doQuery(mapper : SingleColumnMapper[_], analyzer : Analyzer ) : Query = {

        // Check doc values
        if (docValues && !mapper.docValues) {
            throw new IndexException("Field '{}' does not support doc_values", mapper.field)
        } else {
            val clazz : Class[_] = mapper.base;
            if (clazz == classOf[String] || clazz == classOf[Int] || clazz == classOf[Long] || clazz == classOf[Float] || clazz == classOf[Double]) {
                val start = mapper.base(field, lower)
                val stop = mapper.base(field, upper)
                query(start, stop)
            } else {
                throw new IndexException("Range queries are not supported by mapper '{}'", mapper);
            }

        }
    }

    def query(start : Any , stop :Any) : Query = start match {
        case (s:String) =>
            if (docValues)
                DocValuesRangeQuery.newBytesRefRange(field, docValue(start.asInstanceOf[String]), docValue(stop.asInstanceOf[String]), includeLower, includeUpper)
            else
                TermRangeQuery.newStringRange(field, start.asInstanceOf[String], stop.asInstanceOf[String], includeLower, includeUpper)
        case (other) =>
            if (docValues)
                DocValuesRangeQuery.newLongRange(field, docValue(start), docValue(stop), includeLower, includeUpper)
            else other match {
                case (i: Integer) => NumericRangeQuery.newIntRange(field, start.asInstanceOf[Integer], stop.asInstanceOf[Integer], includeLower, includeUpper);
                case (l: Long) => NumericRangeQuery.newLongRange(field, start.asInstanceOf[Long], stop.asInstanceOf[Long], includeLower, includeUpper)
                case (f: Float) => NumericRangeQuery.newFloatRange(field, start.asInstanceOf[Float], stop.asInstanceOf[Float], includeLower, includeUpper)
                case (d: Double) => NumericRangeQuery.newDoubleRange(field, start.asInstanceOf[Double], stop.asInstanceOf[Double], includeLower, includeUpper)
                case (other) =>throw new IndexException("calling RangeCondition.query with invalid types ");
            }
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper =
        toStringHelper(this).add("lower", lower)
                                   .add("upper", upper)
                                   .add("includeLower", includeLower)
                                   .add("includeUpper", includeUpper)
                                   .add("docValues", docValues)

}
object RangeCondition {
    /** The default include lower option. */
    def DEFAULT_INCLUDE_LOWER : Boolean = false

    /** The default include upper option. */
    def DEFAULT_INCLUDE_UPPER : Boolean = false

    /** The default use doc values option. */
    def DEFAULT_DOC_VALUES : Boolean = false
}