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
package com.stratio.cassandra.lucene.search.condition;

import java.util

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.schema.Schema
import com.stratio.cassandra.lucene.util.Logging
import org.apache.lucene.search.BooleanClause.Occur._
import org.apache.lucene.search.{BooleanQuery, MatchAllDocsQuery}

/**
 * A [[Condition]] that matches documents matching boolean combinations of other queries, e.g. [[MatchCondition]]s, [[RangeCondition]]s or other [[BooleanCondition]]s.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param must the mandatory conditions
 * @param should the optional conditions
 * @param not the mandatory not conditions
 */
class BooleanCondition( val boost : java.lang.Float,
                        val must : Array[Condition],
                        val should : Array[Condition],
                        val not : Array[Condition]) extends Condition(boost) with Logging {
    /** @inheritdoc */
    override
    def doQuery(schema: Schema) : BooleanQuery  = {
        val builder : BooleanQuery.Builder  = new BooleanQuery.Builder()
        must.foreach(condition => builder.add(condition.query(schema), MUST))
        should.foreach(condition => builder.add(condition.query(schema), SHOULD))
        not.foreach(condition => builder.add(condition.query(schema), MUST_NOT))
        if (must.isEmpty && should.isEmpty && !not.isEmpty) {
            logger.warn("Performing resource-intensive pure negation query {}", this)
            builder.add(new MatchAllDocsQuery(), FILTER)
        }
        builder.build()
    }

    /** @inheritdoc */
    def postProcessingFields() : util.Set[String] = {
        val fields : util.Set[String] = new util.LinkedHashSet()
        must.foreach(condition => fields.addAll(condition.postProcessingFields))
        should.foreach(condition => fields.addAll(condition.postProcessingFields))
        fields
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper =
        toStringHelper(this).add("must", must).add("should", should).add("not", not)
}
