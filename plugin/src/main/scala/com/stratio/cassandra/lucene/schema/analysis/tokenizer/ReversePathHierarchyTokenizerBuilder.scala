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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  * @param bufferSize if a token length is bigger that this, token is split at max token length intervals.
  * @param delimiter if a token length is bigger that this, token is split at max token length intervals.
  * @param replacement if a token length is bigger that this, token is split at max token length intervals.
  * @param skip if a token length is bigger that this, token is split at max token length intervals.
  */
case class ReversePathHierarchyTokenizerBuilder(@JsonProperty("buffer_size") final val bufferSize: Integer, @JsonProperty(
  "delimiter") final val delimiter: Character, @JsonProperty("replacement") final val replacement: Character, @JsonProperty(
  "skip") final val skip: Integer) extends TokenizerBuilder[ReversePathHierarchyTokenizer] {

  /**
    * Builds a new {@link ReversePathHierarchyTokenizer} using the specified bufferSize, delimiter, replacement and skip values.
    *
    */
  override def function = () => new ReversePathHierarchyTokenizer(getOrDefault(Option(bufferSize), ReversePathHierarchyTokenizerBuilder.DEFAULT_BUFFER_SIZE).asInstanceOf[Int],
                                                                  getOrDefault(Option(delimiter), ReversePathHierarchyTokenizerBuilder.DEFAULT_DELIMITER).asInstanceOf[Char],
                                                                  getOrDefault(Option(replacement), ReversePathHierarchyTokenizerBuilder.DEFAULT_REPLACEMENT).asInstanceOf[Char],
                                                                  getOrDefault(Option(skip), ReversePathHierarchyTokenizerBuilder.DEFAULT_SKIP).asInstanceOf[Int])
}

object ReversePathHierarchyTokenizerBuilder {
  final val DEFAULT_BUFFER_SIZE = 1024
  final val DEFAULT_DELIMITER = '/'
  final val DEFAULT_REPLACEMENT = '/'
  final val DEFAULT_SKIP = 0
}