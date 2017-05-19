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
package com.stratio.cassandra.lucene.schema.analysis;

import com.stratio.cassandra.lucene.IndexException
import org.apache.lucene.analysis.{TokenFilter, TokenStream}
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute
import org.tartarus.snowball.SnowballStemmer
import java.io.IOException
import java.util
import java.util.Arrays;

/**
 * Version of [[ org.apache.lucene.analysis.snowball.SnowballFilter} modified to be compatible with
 * [[ org.tartarus.snowball.SnowballStemmer} 1.3.0.581.1, imposed by SASI indexes.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * Construct the a new snowball filter for the specified language.
 *
 * The supported languages are English, French, Spanish, Portuguese, Italian, Romanian, German, Dutch, Swedish,
 * Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian, Basque and Catalan.
 *
 * @param stream the input tokens stream to be stemmed
 * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
 * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Hungarian and Turkish. Basque and
 * Catalan.
 */
class SnowballFilter(stream: TokenStream, language: String) extends TokenFilter(stream) {
    var stemmer: SnowballStemmer =null
  try {
      stemmer= Class.forName("org.tartarus.snowball.ext." + language.toLowerCase() + "Stemmer")
        .asSubclass(classOf[SnowballStemmer])
      .newInstance()
    }catch {
      case (e: Exception) => throw new IndexException(e, "The specified language '{}' is not valid", language)
    }

    val termAtt : CharTermAttribute = addAttribute(classOf[CharTermAttribute])
    val keywordAttr : KeywordAttribute = addAttribute(classOf[KeywordAttribute])

    /** @inheritdoc */
    override def incrementToken() : Boolean = {
        if (input.incrementToken()) {
            if (!keywordAttr.isKeyword) {
                val termBuffer = termAtt.buffer()
                val length = termAtt.length()
                this.stemmer.setCurrent(new String(util.Arrays.copyOf(termBuffer, length)))
                if (stemmer.stem()) {
                    val finalTerm = stemmer.getCurrent.toCharArray
                    val newLength = finalTerm.length
                    termAtt.copyBuffer(finalTerm, 0, newLength)
                } else {
                    termAtt.setLength(length)
                }
            }
            return true
        }
        false
    }
}