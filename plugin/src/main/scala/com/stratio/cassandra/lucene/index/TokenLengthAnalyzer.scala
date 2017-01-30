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
package com.stratio.cassandra.lucene.index

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.util.Logging
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.FilteringTokenFilter
import org.apache.lucene.analysis.{Analyzer, AnalyzerWrapper}
import org.apache.lucene.index.IndexWriter

/** [[AnalyzerWrapper]] that discards too large tokens.
  *
  * @param analyzer the analyzer to be wrapped
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class TokenLengthAnalyzer(val analyzer: Analyzer)
  extends AnalyzerWrapper(analyzer.getReuseStrategy) with Logging {

  /** inheritdoc */
  override protected def getWrappedAnalyzer(fieldName: String): Analyzer = analyzer

  /** @inheritdoc */
  override protected def wrapComponents(
      field: String,
      components: TokenStreamComponents): TokenStreamComponents = {
    val tokenFilter = new FilteringTokenFilter(components.getTokenStream) {

      val term = addAttribute(classOf[CharTermAttribute])
      val maxSize = IndexWriter.MAX_TERM_LENGTH

      /** @inheritdoc */
      override protected def accept: Boolean = {
        val size = term.length
        if (size <= maxSize) true
        else {
          logger.warn(
            s"Discarding immense term in field='$field', " +
              s"Lucene only allows terms with at most $maxSize bytes in length; got $size")
          false
        }
      }
    }
    new TokenStreamComponents(components.getTokenizer, tokenFilter)
  }

  /** @inheritdoc */
  override def toString: String = {
    MoreObjects.toStringHelper(this).add("analyzer", analyzer).toString
  }

}
