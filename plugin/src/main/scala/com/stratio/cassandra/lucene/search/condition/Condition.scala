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

import java.util

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.Schema
import org.apache.lucene.search.{BoostQuery, Query}
import org.apache.lucene.util.{BytesRef, NumericUtils}

/**
 * The abstract base class for queries.
 *
 * Known subclasses are: <ul> <li> [[AllCondition]] <li> [[BitemporalCondition]] <li> [[ContainsCondition]]
 * <li> [[FuzzyCondition]] <li> [[MatchCondition]] <li> [[PhraseCondition]] <li> [[PrefixCondition]]
 * <li> [[RangeCondition]] <li> [[WildcardCondition]] <li> [[GeoDistanceCondition]] <li>
 *   [[GeoBBoxCondition]] </ul>
 *
 * @param boost_ the boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
  * @author Andres de la Pena `adelapena@stratio.com`
 */
abstract class Condition(val boost_ : Float) {

    /**
     * Returns the Lucene [[Query]] representation of this condition.
     *
     * @param schema the schema to be used
     * @return the Lucene query
     */
    def query( schema:Schema) : Query = {
        val query = doQuery(schema)
        if (boost_ == null) query else new BoostQuery(query, boost_)
    }

    /**
     * Returns the Lucene [[Query]] representation of this condition without boost.
     *
     * @param schema the schema to be used
     * @return the Lucene query
     */
    def doQuery(schema: Schema) :Query

    /**
     * Returns the names of the involved fields.
     *
     * @return the names of the involved fields
     */
    def postProcessingFields: util.Set[String]

    def docValue(value: String ) : BytesRef = if (value == null) null else new BytesRef(value)

    def docValue(value: Any): Long = {
        if (value==null) {
            Long.unbox(null)
        }  else {
            value match {
                case (l: Long) => l
                case (i: Int) => i.longValue()
                case (f: Float) => docValue(NumericUtils.floatToSortableInt(f))
                case (d: Double) => d.longValue()
                case (other) => throw new IndexException(s"calculating doc value form a non valid type $other")
            }
        }
    }

    def toStringHelper(o : Any) : MoreObjects.ToStringHelper = MoreObjects.toStringHelper(o).add("boost", boost_)

    def toStringHelper: MoreObjects.ToStringHelper

    /** @inheritdoc*/
    override def toString: String =
        toStringHelper().toString
}
