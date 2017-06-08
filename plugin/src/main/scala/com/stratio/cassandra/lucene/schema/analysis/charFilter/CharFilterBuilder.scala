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
package com.stratio.cassandra.lucene.schema.analysis.charFilter

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.stratio.cassandra.lucene.schema.analysis.Builder
import org.apache.lucene.analysis.util.CharFilterFactory


/**
  * {@link Builder} for building {@link CharFilterBuilder}s in classpath using its default constructor.
  *
  * Encapsulates all functionality to build Lucene CharFilter. Override 'buildFunction', in Builder trait,
  * to implement the construction of a type of Lucene CharFilterFactory with its parameters and its name
  *
  * @param typeBuilder name of factory in Lucene API
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = CharFilterBuilder.TYPE)
@JsonSubTypes(Array(new Type(value = classOf[HtmlStripCharFilterBuilder], name = CharFilterBuilder.HTML_STRIP),
                    new Type(value = classOf[MappingCharFilterBuilder], name = CharFilterBuilder.MAPPING),
                    new Type(value = classOf[PersianCharFilterBuilder], name = CharFilterBuilder.PERSIAN),
                    new Type(value = classOf[PatternReplaceCharFilterBuilder], name = CharFilterBuilder.PATTERN_REPLACE)))
sealed abstract class CharFilterBuilder[T](typeBuilder: String) extends Builder[T]{
  def buildFunction = () => CharFilterFactory.forName(typeBuilder, mapParsed).asInstanceOf[T]
}

final case class HtmlStripCharFilterBuilder() extends CharFilterBuilder[CharFilterFactory](CharFilterBuilder.HTML_STRIP)
final case class PersianCharFilterBuilder() extends CharFilterBuilder[CharFilterFactory](CharFilterBuilder.PERSIAN)
final case class PatternReplaceCharFilterBuilder(@JsonProperty(CharFilterBuilder.PATTERN) pattern: String, @JsonProperty(CharFilterBuilder.REPLACEMENT)  replacement:String) extends CharFilterBuilder[CharFilterFactory](CharFilterBuilder.PATTERN_REPLACE)
final case class MappingCharFilterBuilder(@JsonProperty(CharFilterBuilder.MAPPINGS) mapping: String) extends CharFilterBuilder[CharFilterFactory](CharFilterBuilder.MAPPING){

}

object CharFilterBuilder{
  final val MAPPINGS = "mapping"
  final val TYPE = "type"
  final val PATTERN = "pattern"
  final val HTML_STRIP = "htmlstrip"
  final val MAPPING = "mapping"
  final val PERSIAN = "persian"
  final val PATTERN_REPLACE = "patternreplace"
  final val REPLACEMENT = "replacement"
}
