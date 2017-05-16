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

import org.apache.lucene.analysis.core.LetterTokenizer

/**
  * A {@link LetterTokenizer} for building {@link org.apache.lucene.analysis.core.LetterTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  */
case class  LetterTokenizerBuilder() extends TokenizerBuilder[LetterTokenizer] {
  /**
    * Builds a new {@link LetterTokenizer}.
    */
  override def function: () => LetterTokenizer = () => new LetterTokenizer
}

object LetterTokenizerBuilder {}


