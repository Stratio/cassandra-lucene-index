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

import java.util.Date

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.common.GeoOperation
import com.stratio.cassandra.lucene.schema.mapping.DateRangeMapper
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.Query
import org.apache.lucene.spatial.SpatialStrategy
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape
import org.apache.lucene.spatial.query.{SpatialArgs, SpatialOperation}

/**
 * A [[Condition]] implementation that matches a field within an range of values.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param field the name of the field to be matched
 * @param from the lower accepted [[Date]]. Maybe {{{null} meaning no lower limit
 * @param to the upper accepted [[Date]]. Maybe {{{null} meaning no upper limit
 * @param operation the spatial operation to be performed
 */
class DateRangeCondition(val boost : java.lang.Float,
                         val field: String,
                         var from: Any,
                         var to: Any,
                         var operation: GeoOperation) extends SingleMapperCondition[DateRangeMapper](boost, field, classOf[DateRangeMapper]) {

    from = if (from == null) DateRangeCondition.DEFAULT_FROM else from
    to = if (to == null) DateRangeCondition.DEFAULT_TO else to
    operation = if (operation == null) DateRangeCondition.DEFAULT_OPERATION else operation


    /** @inheritdoc */
    override def doQuery(mapper : DateRangeMapper, analyzer : Analyzer) : Query = {
        val strategy : SpatialStrategy = mapper.strategy

        val fromDate : Date = mapper.base(from)
        val toDate : Date = mapper.base(to)

        val shape : NRShape = mapper.makeShape(fromDate, toDate)
        val args : SpatialArgs = new SpatialArgs(operation.spatialOperation, shape)
        strategy.makeQuery(args)
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper =
        toStringHelper(this).add("from", from).add("to", to).add("operation", operation)

}

object DateRangeCondition {

    /** The default from value. */
    val DEFAULT_FROM :  Date = new Date(Long.MinValue)

    /** The default to value. */
    val DEFAULT_TO = new Date(Long.MaxValue)

    /** The default operation. */
    val DEFAULT_OPERATION : GeoOperation = GeoOperation.INTERSECTS
}