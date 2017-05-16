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
import org.apache.lucene.analysis.standard.ClassicTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.standard.ClassicTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  * @param maxTokenLength if a token length is bigger that this, token is split at max token length intervals.
  */
case class ClassicTokenizerBuilder(@JsonProperty("max_token_length") maxTokenLength: Integer) extends TokenizerBuilder[ClassicTokenizer] {

  /**
    * Gets or creates the Lucene {@link Tokenizer}.
    *
    * @return the built analyzer
    */
  override val function = () => {
    val tokenizer = new ClassicTokenizer()
    tokenizer.setMaxTokenLength(getOrDefault(Option(maxTokenLength),
      ClassicTokenizerBuilder.DEFAULT_MAX_TOKEN_LENGTH).asInstanceOf[Integer])
    tokenizer
  }
}

object ClassicTokenizerBuilder {
  final val DEFAULT_MAX_TOKEN_LENGTH = 250
}