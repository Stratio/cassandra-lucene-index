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
package com.stratio.cassandra.lucene.mapping

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.search.{Search, SearchBuilder}
import org.apache.cassandra.config.{CFMetaData, ColumnDefinition}
import org.apache.cassandra.cql3.Operator
import org.apache.cassandra.cql3.statements.IndexTarget._
import org.apache.cassandra.db.ReadCommand
import org.apache.cassandra.db.filter.RowFilter
import org.apache.cassandra.db.filter.RowFilter.CustomExpression
import org.apache.cassandra.db.marshal.UTF8Type
import org.apache.cassandra.db.rows.{BTreeRow, BufferCell, Row}
import org.apache.cassandra.schema.IndexMetadata
import org.apache.commons.lang3.StringUtils
import org.apache.lucene.search.ScoreDoc
import org.apache.cassandra.db.filter.RowFilter.Expression

import scala.collection.JavaConversions._
import com.stratio.cassandra.lucene.mapping.ExpressionMapper.parse

/** Class for several [[Expression]] mappings between Cassandra and Lucene.
  *
  * @param tableMetadata the indexed table metadata
  * @param indexMetadata the index metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
case class ExpressionMapper(tableMetadata: CFMetaData, indexMetadata: IndexMetadata) {

  val name = indexMetadata.name
  val column = Option(indexMetadata.options.get(TARGET_OPTION_NAME)).filterNot(StringUtils.isBlank)
  val columns = tableMetadata.allColumns.toSet
  val columnDefinition = column.flatMap(name => columns.find(_.name.toString == name))

  def search(command: ReadCommand): Search = {
    parse(json(command))
  }

  def search(expression: Expression): Search = {
    parse(json(expression))
  }

  def json(command: ReadCommand): String = {
    command.rowFilter.getExpressions.collect {
      case e: CustomExpression if name == e.getTargetIndex.name => e.getValue
      case e if supports(e) => e.getIndexValue
    }.map(UTF8Type.instance.compose).head
  }

  def json(expression: Expression): String = {
    UTF8Type.instance.compose(expression match {
      case e: CustomExpression if name == e.getTargetIndex.name => e.getValue
      case e if supports(e) => e.getIndexValue
      case _ => throw new IndexException(s"Unsupported expression $expression")
    })
  }

  /** Returns if the specified expression is targeted to this index
    *
    * @param expression a CQL query expression
    * @return `true` if `expression` is targeted to this index, `false` otherwise
    */
  def supports(expression: RowFilter.Expression): Boolean = {
    supports(expression.column, expression.operator)
  }

  /** Returns if a CQL expression with the specified column definition and operator is targeted to
    * this index.
    *
    * @param columnDef the expression column definition
    * @param operator  the expression operator
    * @return `true` if the expression is targeted to this index, `false` otherwise
    */
  def supports(columnDef: ColumnDefinition, operator: Operator): Boolean = {
    operator == Operator.EQ && column.contains(columnDef.name.toString)
  }

  /** Returns a copy of the specified [[RowFilter]] without any Lucene expressions.
    *
    * @param filter a row filter
    * @return a copy of `filter` without Lucene expressions
    */
  def postIndexQueryFilter(filter: RowFilter): RowFilter = {
    if (column.isEmpty) return filter
    filter.foldLeft(filter)((f, e) => if (supports(e)) f.without(e) else f)
  }

  def decorate(row: Row, score: ScoreDoc, nowInSec: Int): Row = {

    // Skip if there is no base column or score
    if (columnDefinition.isEmpty) return row

    // Copy row
    val builder = BTreeRow.unsortedBuilder(nowInSec)
    builder.newRow(row.clustering())
    builder.addRowDeletion(row.deletion)
    builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo)
    row.cells.foreach(builder.addCell)

    // Add score cell
    val timestamp = row.primaryKeyLivenessInfo.timestamp
    val scoreCellValue = UTF8Type.instance.decompose(score.score.toString)
    builder.addCell(BufferCell.live(tableMetadata, columnDefinition.get, timestamp, scoreCellValue))

    builder.build
  }
}

object ExpressionMapper {

  def parse(json: String): Search = {
    SearchBuilder.fromJson(json).build
  }

}