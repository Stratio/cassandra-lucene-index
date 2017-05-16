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
package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import java.util.regex.Pattern

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.lucene.analysis.pattern.PatternTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.pattern.PatternTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com>}
  * @param pattern if a token length is bigger that this, token is split at max token length intervals.
  * @param flags if a token length is bigger that this, token is split at max token length intervals.
  * @param group if a token length is bigger that this, token is split at max token length intervals.
  */
case class PatternTokenizerBuilder(@JsonProperty("pattern") final val pattern: String, @JsonProperty(
  "flags") final val flags: Integer, @JsonProperty("group") final val group: Integer) extends TokenizerBuilder[PatternTokenizer] {
  /**
    * Builds a new {@link KeywordTokenizerBuilder} using the specified maxTokenLength.
    *
    */
    override def function = () =>  new PatternTokenizer(Pattern.compile(
      getOrDefault(Option(pattern), PatternTokenizerBuilder.DEFAULT_PATTERN).asInstanceOf[String],
      getOrDefault(Option(flags), PatternTokenizerBuilder.DEFAULT_FLAGS).asInstanceOf[Integer]),
      getOrDefault(Option(group), PatternTokenizerBuilder.DEFAULT_GROUP).asInstanceOf[Integer])
}


object PatternTokenizerBuilder {
  final val DEFAULT_PATTERN = "\\W+"
  final val DEFAULT_FLAGS = 0
  final val DEFAULT_GROUP = -1
}