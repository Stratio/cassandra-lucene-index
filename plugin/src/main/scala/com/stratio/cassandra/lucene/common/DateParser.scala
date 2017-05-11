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
package com.stratio.cassandra.lucene.common

import com.stratio.cassandra.lucene.IndexException
import org.apache.cassandra.utils.UUIDGen
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Date
import java.util.UUID
import java.util.function.Supplier

/**
 * Unified class for parsing [[Date]]s from [[Object}s.
 *
 * @author Eduardo Alonso `eduardoalonso@stratio.com`
 */
class DateParser(val pattern_param : String ) {

    val pattern = if (pattern_param==null) DateParser.DEFAULT_PATTERN else pattern_param
    /** The thread safe date format. */
    val formatter: ThreadLocal[DateFormat]=DateParser.formatter(pattern)

    def getPattern: String = pattern
    /**
     * Returns the [[Date]] represented by the specified [[Any]], or [[null]] if the specified
     * [[Object]] is [[null]].
     *
     * @param value the [[Any]] to be parsed
     * @return the parsed [[Date]]
     */
    def parse(value: Any): Date = {
      try {
        if (value == null) {
             null
        } else value match {
          case date: Date => DateParser.parse(formatter,date)
          case uuid: UUID => DateParser.parse(formatter,uuid)
          case number : Number => DateParser.parse(formatter, number)
          case _ : Any => DateParser.parse(formatter, value)
        }
      } catch {
        case e: Exception =>
          throw new IndexException(e, s"Error parsing ${value.getClass} with value '$value' using date pattern $pattern");
      }
    }

    def toString(date : Date): String = formatter.get().format(date)

    override def toString : String = pattern
}


object DateParser {
    /** The default date pattern for parsing [[String]]s and truncations. */
  val DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS Z"

  def formatter(pattern : String) : ThreadLocal[DateFormat] ={
    new SimpleDateFormat(pattern)
    val formatter : ThreadLocal[DateFormat]= ThreadLocal.withInitial( new DateParserSupplier(pattern))
    formatter.get().setLenient(false)
    formatter
  }

  def parse(formatter: ThreadLocal[DateFormat], date : Date) : Date = {
    if (date.getTime == Long.MaxValue || date.getTime == Long.MinValue) date
      else formatter.get().parse(formatter.get().format(date))
  }

  def parse(formatter: ThreadLocal[DateFormat], date : UUID) : Date =
    formatter.get().parse(formatter.get().format(new Date(UUIDGen.unixTimestamp(date))))

  def parse(formatter: ThreadLocal[DateFormat], date : Number) : Date = date match {
    case _ => formatter.get().parse(date.longValue().toString)
  }

  def parse(formatter: ThreadLocal[DateFormat], date : Any) : Date = date match {
    case _ => formatter.get().parse(date.toString)
  }

  class DateParserSupplier(pattern: String) extends Supplier[DateFormat] {
    override def get(): DateFormat = new SimpleDateFormat(pattern)
  }
}
