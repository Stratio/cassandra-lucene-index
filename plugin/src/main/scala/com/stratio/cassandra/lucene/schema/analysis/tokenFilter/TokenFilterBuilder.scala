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
package com.stratio.cassandra.lucene.schema.analysis.tokenFilter

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.stratio.cassandra.lucene.schema.analysis.Builder
import org.apache.lucene.analysis.util.TokenFilterFactory


/**
  * {@link Builder} for building {@link TokenFilterBuilder}s in classpath using its default constructor.
  *
  * Encapsulates all functionality to build Lucene TokenFilter. Override 'buildFunction', in Builder trait,
  * to implement the construction of a type of Lucene TokenFilterFactory with its parameters and its name
  *
  * @param typeBuilder name of factory in Lucene API
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(Array(new Type(value = classOf[StandardTokenFilterBuilder], name = "standard"),
  new Type(value = classOf[ApostropheTokenFilterBuilder], name = "apostrophe"),
  new Type(value = classOf[ArabicNormalizationTokenFilterBuilder], name = "arabicnormalization"),
  new Type(value = classOf[ArabicStemTokenFilterBuilder], name = "arabicstem"),
  new Type(value = classOf[SoraniNormalizationTokenFilterBuilder], name = "soraninormalization"),
  new Type(value = classOf[IndicNormalizationTokenFilterBuilder], name = "indicnormalization"),
  new Type(value = classOf[PortugueseStemTokenFilterBuilder], name = "portuguesestem"),
  new Type(value = classOf[GermanMinimalStemTokenFilterBuilder], name = "germanminimalstem"),
  new Type(value = classOf[UpperCaseTokenFilterBuilder], name = "uppercase"),
  new Type(value = classOf[KeywordRepeatTokenFilterBuilder], name = "keywordrepeat"),
  new Type(value = classOf[ClassicTokenFilterBuilder], name = "classic"),
  new Type(value = classOf[ShingleTokenFilterBuilder], name = "shingle"),
  new Type(value = classOf[StemmeroverrideTokenFilterBuilder], name = "stemmeroverride"),
  new Type(value = classOf[BulgarianstemTokenFilterBuilder], name = "bulgarianstem"),
  new Type(value = classOf[SwedishlightstemTokenFilterBuilder], name = "swedishlightstem"),
  new Type(value = classOf[FrenchlightstemTokenFilterBuilder], name = "frenchlightstem"),
  new Type(value = classOf[CjkwidthTokenFilterBuilder], name = "cjkwidth"),
  new Type(value = classOf[GreekstemTokenFilterBuilder], name = "greekstem"),
  new Type(value = classOf[StopTokenFilterBuilder], name = "stop"),
  new Type(value = classOf[HindistemTokenFilterBuilder], name = "hindistem"),
  new Type(value = classOf[FingerprintTokenFilterBuilder], name = "fingerprint"),
  new Type(value = classOf[SpanishlightstemTokenFilterBuilder], name = "spanishlightstem"),
  new Type(value = classOf[HungarianlightstemTokenFilterBuilder], name = "hungarianlightstem"),
  new Type(value = classOf[NorwegianminimalstemTokenFilterBuilder], name = "norwegianminimalstem"),
  new Type(value = classOf[PersiannormalizationTokenFilterBuilder], name = "persiannormalization"),
  new Type(value = classOf[GermanlightstemTokenFilterBuilder], name = "germanlightstem"),
  new Type(value = classOf[TypeTokenFilterBuilder], name = "type"),
  new Type(value = classOf[AsciifoldingTokenFilter], name = "asciifolding"),
  new Type(value = classOf[LowercaseTokenFilter], name = "lowercase")
))
sealed abstract class TokenFilterBuilder[T](typeBuilder: String) extends Builder[T]{
  /** {@inheritDoc} */
  def buildFunction = () => TokenFilterFactory.forName(typeBuilder, mapParsed).asInstanceOf[T]
}

final case class StandardTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("standard")
final case class ApostropheTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("apostrophe")
final case class ArabicNormalizationTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("arabicnormalization")
final case class ArabicStemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("arabicstem")
final case class SoraniNormalizationTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("soraninormalization")
final case class IndicNormalizationTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("indicnormalization")
final case class PortugueseStemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("portuguesestem")
final case class GermanMinimalStemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("germanminimalstem")
final case class UpperCaseTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("uppercase")
final case class KeywordRepeatTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("keywordrepeat")
final case class ClassicTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("classic")
final case class ShingleTokenFilterBuilder(@JsonProperty("min_shingle_size") minShingleSize: Integer, @JsonProperty("max_shingle_size") maxShingleSize: Integer) extends TokenFilterBuilder[TokenFilterFactory]("shingle")
final case class StemmeroverrideTokenFilterBuilder(@JsonProperty("dictionary") dictionary: String, @JsonProperty("ignore_case") ignoreCase: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("stemmeroverride")
final case class BulgarianstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("bulgarianstem")
final case class SwedishlightstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("swedishlightstem")
final case class FrenchlightstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("frenchlightstem")
final case class CjkwidthTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("cjkwidth")
final case class GreekstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("greekstem")
final case class StopTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("stop")
final case class HindistemTokenFilterBuilder(@JsonProperty("maxOutputTokenSize") maxOutputTokenSize: Integer, @JsonProperty("separator") separator: String) extends TokenFilterBuilder[TokenFilterFactory]("hindistem")
final case class FingerprintTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("fingerprint")
final case class SpanishlightstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("spanishlightstem")
final case class HungarianlightstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("hungarianlightstem")
final case class NorwegianminimalstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("norwegianminimalstem")
final case class PersiannormalizationTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("persiannormalization")
final case class GermanlightstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("germanlightstem")
final case class TypeTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("type")
//
final case class AsciifoldingTokenFilter(@JsonProperty("preserve_original") preserveOriginal:Boolean) extends TokenFilterBuilder[TokenFilterFactory]("asciifolding")
final case class LowercaseTokenFilter() extends TokenFilterBuilder[TokenFilterFactory]("lowercase")
//
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("germanstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("ngram")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("limittokenposition")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("greeklowercase")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("standard")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("limittokenoffset")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("snowballporter")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("typeaspayload")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("patternreplace")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("cjkbigram")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("keywordmarker")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("soranistem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("elision")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("hunspellstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("codepointcount")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("czechstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("turkishlowercase")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("daterecognizer")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("portugueselightstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("irishlowercase")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("commongramsquery")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("numericpayload")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("scandinavianfolding")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("germannormalization")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("delimitedpayload")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("worddelimiter")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("portugueseminimalstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("removeduplicates")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("edgengram")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("latvianstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("porterstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("finnishlightstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("commongrams")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("galicianstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("kstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("reversestring")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("asciifolding")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("norwegianlightstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("trim")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("length")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("decimaldigit")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("brazilianstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("capitalization")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("serbiannormalization")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("frenchminimalstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("englishminimalstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("limittokencount")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("hyphenatedwords")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("truncate")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("tokenoffsetpayload")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("galicianminimalstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("russianlightstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("englishpossessive")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("lowercase")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("hindinormalization")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("scandinaviannormalization")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("thaiword")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("synonym")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("indonesianstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("keepword")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("hyphenationcompoundword")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("dictionarycompoundword")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("italianlightstem")
//final case class StandardTokenFilter() extends TokenizerFilterBuilder[TokenFilterFactory]("patterncapturegroup")
