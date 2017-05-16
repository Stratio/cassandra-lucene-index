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

import org.apache.lucene.analysis.core.UnicodeWhitespaceTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.core.UnicodeWhitespaceTokenizer}
  *
  * @author Eduardo Alonso eduardoalonso@stratio.com
  */
case class UnicodeWhitespaceTokenizerBuilder() extends TokenizerBuilder[UnicodeWhitespaceTokenizer] {
  /**
    * Builds a new {@link UnicodeWhitespaceTokenizer}.
    */
    override def function = () => new UnicodeWhitespaceTokenizer
}

object UnicodeWhitespaceTokenizerBuilder {}