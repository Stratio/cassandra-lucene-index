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
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer}
  *
  * @author Eduardo Alonso eduardoalonso@stratio.com
  * @param maxTokenLength if a token length is bigger that this, token is split at max token length intervals.
  */
case class UAX29URLEmailTokenizerBuilder(@JsonProperty("max_token_length") final val maxTokenLength: Integer) extends TokenizerBuilder[UAX29URLEmailTokenizer] {
  /**
    * Builds a new {@link UAX29URLEmailTokenizer} using the specified maxTokenLength.
    *
    */
    override def function = () => {
      val tokenizer: UAX29URLEmailTokenizer = new UAX29URLEmailTokenizer
      tokenizer.setMaxTokenLength(getOrDefault(Option(maxTokenLength), UAX29URLEmailTokenizerBuilder.DEFAULT_MAX_TOKEN_LENGHT).asInstanceOf[Int])
      tokenizer
    }
}

object UAX29URLEmailTokenizerBuilder {
  final val DEFAULT_MAX_TOKEN_LENGHT = 255
}