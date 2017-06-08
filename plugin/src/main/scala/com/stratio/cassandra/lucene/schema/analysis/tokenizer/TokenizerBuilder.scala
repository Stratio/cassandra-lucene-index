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
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.stratio.cassandra.lucene.schema.analysis.Builder
import org.apache.lucene.analysis.util.TokenizerFactory


/**
  * {@link Builder} for building {@link TokenizerBuilder}s in classpath using its default constructor.
  *
  * Encapsulates all functionality to build Lucene Tokenizer. Override 'buildFunction', in Builder trait,
  * to implement the construction of a type of Lucene TokenizerFactory with its parameters and its name
  *
  * @param typeBuilder name of factory in Lucene API
  *
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
                    new Type(value = classOf[StandardTokenizerBuilder], name = "standard"),
                    new Type(value = classOf[UAX29URLEmailTokenizerBuilder], name = "uax29_url_email"),
                    new Type(value = classOf[ThaiTokenizerBuilder], name = "thai"),
                    new Type(value = classOf[WhitespaceTokenizerBuilder], name = "whitespace"),
                    new Type(value = classOf[WikipediaTokenizerBuilder], name = "wikipedia"))
) sealed abstract class TokenizerBuilder[T](typeBuilder: String) extends Builder[T]{
    /** {@inheritDoc} */
    def buildFunction = () => TokenizerFactory.forName(typeBuilder, mapParsed).asInstanceOf[T]
}

final case class ClassicTokenizerBuilder(@JsonProperty("max_token_length") maxTokenLength: Integer) extends TokenizerBuilder[TokenizerFactory]("classic")
final case class EdgeNGramTokenizerBuilder(@JsonProperty("min_gram_size") minGramSize: Integer, @JsonProperty("max_gram_size") maxGramSize: Integer) extends TokenizerBuilder[TokenizerFactory]("edgengram")
final case class KeywordTokenizerBuilder() extends TokenizerBuilder[TokenizerFactory]("keyword")
final case class LetterTokenizerBuilder() extends TokenizerBuilder[TokenizerFactory]("letter")
final case class LowerCaseTokenizerBuilder() extends TokenizerBuilder[TokenizerFactory]("lowercase")
final case class NGramTokenizerBuilder(@JsonProperty("min_gram_size") minGramSize: Integer, @JsonProperty("max_gram_size") maxGramSize: Integer) extends TokenizerBuilder[TokenizerFactory]("ngram")
final case class PathHierarchyTokenizerBuilder(@JsonProperty("reverse") reverse: Boolean, @JsonProperty("delimiter") delimiter: Char, @JsonProperty("replace") replace: Char, @JsonProperty("skip") skip: Integer) extends TokenizerBuilder[TokenizerFactory]("pathhierarchy")
final case class PatternTokenizerBuilder(@JsonProperty("pattern") pattern: String, @JsonProperty("group") group: Integer) extends TokenizerBuilder[TokenizerFactory]("pattern")
final case class StandardTokenizerBuilder(@JsonProperty("max_token_length") maxTokenLength: Integer) extends TokenizerBuilder[TokenizerFactory]("standard")
final case class ThaiTokenizerBuilder() extends TokenizerBuilder[TokenizerFactory]("thai")
final case class UAX29URLEmailTokenizerBuilder(@JsonProperty("max_token_length") maxTokenLength: Integer) extends TokenizerBuilder[TokenizerFactory]("uax29urlemail")
final case class WhitespaceTokenizerBuilder(@JsonProperty("rule") rule: String) extends TokenizerBuilder[TokenizerFactory]("whitespace")
final case class WikipediaTokenizerBuilder() extends TokenizerBuilder[TokenizerFactory]("wikipedia")
