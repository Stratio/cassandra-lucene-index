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
import org.apache.commons.lang3.StringUtils
import java.util.Collections

/**
 * The abstract base class for queries directed to a specific field which name should be specified.
 *
 * Known subclasses are: <ul> <li> [[FuzzyCondition]] <li> [[MatchCondition]] <li> [[PhraseCondition]] <li>
 * [[PrefixCondition]] <li> [[RangeCondition]] <li> [[WildcardCondition]] <li> [[BitemporalCondition]]
 * <li> [[DateRangeCondition]] <li> [[GeoDistanceCondition]] <li> [[GeoBBoxCondition]] <li> [[GeoShapeCondition]] </ul>
 *
 * @author Andres de la Pena `adelapena@stratio.com`

 * @param boost__ the boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param field_ the name of the field to be matched
 */
abstract class SingleFieldCondition(val boost__ : Float, val field_ : String) extends Condition(boost__) {
    if (StringUtils.isBlank(field_))  throw new IndexException("Field name required")

    /** @inheritdoc */
    def postProcessingFields() : util.Set[String] = Collections.singleton(field_)


    /** @inheritdoc */
    override def toStringHelper(o : Any) : MoreObjects.ToStringHelper = super.toStringHelper(o).add("field", field_)
}
