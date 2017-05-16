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

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import org.apache.lucene.analysis.Tokenizer

/**
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(Array(new Type(value = classOf[ClassicTokenizerBuilder], name = "classic"),
                    new Type(value = classOf[EdgeNGramTokenizerBuilder], name = "edge_ngram"),
                    new Type(value = classOf[KeywordTokenizerBuilder], name = "keyword"),
                    new Type(value = classOf[LetterTokenizerBuilder], name = "letter"),
                    new Type(value = classOf[LowerCaseTokenizerBuilder], name = "lower_case"),
                    new Type(value = classOf[NGramTokenizerBuilder], name = "ngram"),
                    new Type(value = classOf[PathHierarchyTokenizerBuilder], name = "path_hierarchy"),
                    new Type(value = classOf[PatternTokenizerBuilder], name = "pattern"),
                    new Type(value = classOf[ReversePathHierarchyTokenizerBuilder], name = "reverse_path_hierarchy"),
                    new Type(value = classOf[StandardTokenizerBuilder], name = "standard"),
                    new Type(value = classOf[UAX29URLEmailTokenizerBuilder], name = "uax29_url_email"),
                    new Type(value = classOf[UnicodeWhitespaceTokenizerBuilder], name = "unicode_whitespace"),
                    new Type(value = classOf[ThaiTokenizerBuilder], name = "thai"),
                    new Type(value = classOf[WhitespaceTokenizerBuilder], name = "whitespace"),
                    new Type(value = classOf[WikipediaTokenizerBuilder], name = "wikipedia"))
) trait TokenizerBuilder[T <: Tokenizer] {
  /**
    *
    * @return
    */
  def function : ()=>T

  //TODO: refactor scala style (remove throw)
  /**
    *
    * @param throwable
    * @return
    */
  def failThrowException(throwable: Throwable) = throw throwable

  /**
    * Gets or creates the Lucene {@link Tokenizer}.
    *
    * @return the built analyzer
    */
  def buildTokenizer: T = {
    import scala.util.control.Exception._
    //TODO: refactor scala style (manage either in other level)
    catching(classOf[Exception]).either(function()).asInstanceOf[Either[Exception, T]].fold(failThrowException, x=>x)
  }

  /**
    * @param param        the main parameter.
    * @param defaultParam the default parameter if main paramaeter is null.
    * @return if (param!=null) { return param; }else{ return defaultParam; }
    */
  def getOrDefault(param: Option[Any], defaultParam: Any): Any = param.map(x => x).getOrElse(defaultParam)
}

object TokenizerBuilder{}