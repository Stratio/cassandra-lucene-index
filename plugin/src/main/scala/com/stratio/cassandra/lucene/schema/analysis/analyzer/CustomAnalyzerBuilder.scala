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
package com.stratio.cassandra.lucene.schema.analysis.analyzer

import java.io.StringReader
import java.util
import com.fasterxml.jackson.annotation.JsonProperty
import com.stratio.cassandra.lucene.schema.analysis.AnalyzerBuilder
import com.stratio.cassandra.lucene.schema.analysis.charFilter.CharFilterBuilder
import com.stratio.cassandra.lucene.schema.analysis.tokenFilter.TokenFilterBuilder
import com.stratio.cassandra.lucene.schema.analysis.tokenizer.TokenizerBuilder
import org.apache.lucene.analysis.{TokenStream, Analyzer}
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.analysis.util.{CharFilterFactory, TokenFilterFactory, TokenizerFactory}

import scala.util.{Failure, Success, Try}


/**
  * Created by jpgilaberte on 24/05/17.
  */
final case class CustomAnalyzerBuilder( @JsonProperty("tokenizer") tokenizer: TokenizerBuilder[_],
                                        @JsonProperty("char_filter") charFilter: Array[CharFilterBuilder[_]],
                                        @JsonProperty("token_filter") tokenFilter: Array[TokenFilterBuilder[_]]
                                        ) extends AnalyzerBuilder{

  override def analyzer(): Analyzer = {
    val custom = CustomAnalyzer.builder()

    def validateTokenizer(tkf: TokenizerFactory) = tkf.create() // Validate params before index creation
    def addTokenizerFactory(tkf: TokenizerFactory) = custom.withTokenizer(tkf.getClass, new util.HashMap[String, String](tkf.getOriginalArgs))
    Try (tokenizer.build.asInstanceOf[TokenizerFactory]) match {
      case Success(tokenizerFactory) => {validateTokenizer(tokenizerFactory); addTokenizerFactory(tokenizerFactory)}
      case Failure(e) => {/*Tokenizer are mandatory. Validate if it is present in CustomAnalyzer layer*/}
    }

    def validateTokenFilter(tkff: TokenFilterFactory) = tkff.create(new TokenStream() {override def incrementToken(): Boolean = true }) // Validate params before index creation
    def addTokenFilterFactory(tkff: TokenFilterFactory) = custom.addTokenFilter(tkff.getClass, new util.HashMap[String, String](tkff.getOriginalArgs))
    tokenFilter match {
      case null => {/*java legacy*/}
      case _ => { tokenFilter.map(x => {
          val tokenFilter = x.build.asInstanceOf[TokenFilterFactory]
          validateTokenFilter(tokenFilter)
          addTokenFilterFactory(tokenFilter)
        })
      }
    }

    def validateCharFilter(cff: CharFilterFactory) = cff.create(new StringReader("validate")) // Validate params before index creation
    def addCharFilterFactory(cff: CharFilterFactory) = custom.addCharFilter(cff.getClass, new util.HashMap[String, String](cff.getOriginalArgs))
    charFilter match {
      case null => {/*java legacy*/}
      case _ => { charFilter.map(x => {
          val charFilter = x.build.asInstanceOf[CharFilterFactory]
          validateCharFilter(charFilter)
          addCharFilterFactory(charFilter)
        })
      }
    }

    custom.build()
  }
}
