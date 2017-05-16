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
import org.apache.lucene.analysis.path.PathHierarchyTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.path.PathHierarchyTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  * @param bufferSize  terms cache read buffer size
  * @param delimiter   path separator
  * @param replacement a replacement character for delimiter
  * @param skip        number of initial tokens to skip
  */
case class PathHierarchyTokenizerBuilder(@JsonProperty("buffer_size") final val bufferSize: Integer,
                                         @JsonProperty("delimiter") final val delimiter: Character,
                                         @JsonProperty("replacement") final val replacement: Character,
                                         @JsonProperty("skip") final val skip: Integer) extends TokenizerBuilder[PathHierarchyTokenizer] {
  /**
    * Builds a new {@link PathHierarchyTokenizer} using the specified bufferSize, delimiter, replacement and
    * skip.
    */
  override def function = () => new PathHierarchyTokenizer(getOrDefault(Option(bufferSize), PathHierarchyTokenizerBuilder.DEFAULT_BUFFER_SIZE).asInstanceOf[Integer],
                                                           getOrDefault(Option(delimiter), PathHierarchyTokenizerBuilder.DEFAULT_DELIMITER).asInstanceOf[Char],
                                                           getOrDefault(Option(replacement), PathHierarchyTokenizerBuilder.DEFAULT_REPLACEMENT).asInstanceOf[Char],
                                                           getOrDefault(Option(skip), PathHierarchyTokenizerBuilder.DEFAULT_SKIP).asInstanceOf[Integer])
}

object PathHierarchyTokenizerBuilder {
  final val DEFAULT_BUFFER_SIZE = 1024
  final val DEFAULT_DELIMITER = '/'
  final val DEFAULT_REPLACEMENT = '/'
  final val DEFAULT_SKIP = 0
}