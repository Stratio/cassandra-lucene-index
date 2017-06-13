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
@JsonSubTypes(Array(
  new Type(value = classOf[StandardTokenFilterBuilder], name = "standard"),
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
  new Type(value = classOf[GermanstemTokenFilterBuilder], name = "germanstem"),
  new Type(value = classOf[NGramTokenFilterBuilder], name = "ngram"),
  new Type(value = classOf[LimittokenpositionTokenFilterBuilder], name = "limittokenposition"),
  new Type(value = classOf[GreeklowercaseTokenFilterBuilder], name = "greeklowercase"),
  new Type(value = classOf[LimittokenoffsetTokenFilterBuilder], name = "limittokenoffset"),
  new Type(value = classOf[SnowballporterTokenFilterBuilder], name = "snowballporter"),
  new Type(value = classOf[TypeaspayloadTokenFilterBuilder], name = "typeaspayload"),
  new Type(value = classOf[PatternreplaceTokenFilterBuilder], name = "patternreplace"),
  new Type(value = classOf[CjkbigramTokenFilterBuilder], name = "cjkbigram"),
  new Type(value = classOf[KeywordmarkerTokenFilterBuilder], name = "keywordmarker"),
  new Type(value = classOf[SoranistemTokenFilterBuilder], name = "soranistem"),
  new Type(value = classOf[ElisionTokenFilterBuilder], name = "elision"),
  new Type(value = classOf[HunspellstemTokenFilterBuilder], name = "hunspellstem"),
  new Type(value = classOf[CodepointcountTokenFilterBuilder], name = "codepointcount"),
  new Type(value = classOf[CzechstemTokenFilterBuilder], name = "czechstem"),
  new Type(value = classOf[TurkishlowercaseTokenFilterBuilder], name = "turkishlowercase"),
  new Type(value = classOf[DaterecognizerTokenFilterBuilder], name = "daterecognizer"),
  new Type(value = classOf[PortugueselightstemTokenFilterBuilder], name = "portugueselightstem"),
  new Type(value = classOf[IrishlowercaseTokenFilterBuilder], name = "irishlowercase"),
  new Type(value = classOf[CommongramsqueryTokenFilterBuilder], name = "commongramsquery"),
  new Type(value = classOf[NumericpayloadTokenFilterBuilder], name = "numericpayload"),
  new Type(value = classOf[ScandinavianfoldingTokenFilterBuilder], name = "scandinavianfolding"),
  new Type(value = classOf[GermannormalizationTokenFilterBuilder], name = "germannormalization"),
  new Type(value = classOf[DelimitedpayloadTokenFilterBuilder], name = "delimitedpayload"),
  new Type(value = classOf[WorddelimiterTokenFilterBuilder], name = "worddelimiter"),
  new Type(value = classOf[PortugueseminimalstemTokenFilterBuilder], name = "portugueseminimalstem"),
  new Type(value = classOf[RemoveduplicatesTokenFilterBuilder], name = "removeduplicates"),
  new Type(value = classOf[EdgengramTokenFilterBuilder], name = "edgengram"),
  new Type(value = classOf[LatvianstemTokenFilterBuilder], name = "latvianstem"),
  new Type(value = classOf[PorterstemTokenFilterBuilder], name = "porterstem"),
  new Type(value = classOf[FinnishlightstemTokenFilterBuilder], name = "finnishlightstem"),
  new Type(value = classOf[CommongramsTokenFilterBuilder], name = "commongrams"),
  new Type(value = classOf[GalicianstemTokenFilterBuilder], name = "galicianstem"),
  new Type(value = classOf[KstemTokenFilterBuilder], name = "kstem"),
  new Type(value = classOf[AsciifoldingTokenFilterBuilder], name = "asciifolding"),
  new Type(value = classOf[NorwegianlightstemTokenFilterBuilder], name = "norwegianlightstem"),
  new Type(value = classOf[TrimTokenFilterBuilder], name = "trim"),
  new Type(value = classOf[LengthTokenFilterBuilder], name = "length"),
  new Type(value = classOf[DecimaldigitTokenFilterBuilder], name = "decimaldigit"),
  new Type(value = classOf[BrazilianstemTokenFilterBuilder], name = "brazilianstem"),
  new Type(value = classOf[CapitalizationTokenFilterBuilder], name = "capitalization"),
  new Type(value = classOf[SerbiannormalizationTokenFilterBuilder], name = "serbiannormalization"),
  new Type(value = classOf[FrenchminimalstemTokenFilterBuilder], name = "frenchminimalstem"),
  new Type(value = classOf[EnglishminimalstemTokenFilterBuilder], name = "englishminimalstem"),
  new Type(value = classOf[LimittokencountTokenFilterBuilder], name = "limittokencount"),
  new Type(value = classOf[HyphenatedwordsTokenFilterBuilder], name = "hyphenatedwords"),
  new Type(value = classOf[TruncateTokenFilterBuilder], name = "truncate"),
  new Type(value = classOf[TokenoffsetpayloadTokenFilterBuilder], name = "tokenoffsetpayload"),
  new Type(value = classOf[GalicianminimalstemTokenFilterBuilder], name = "galicianminimalstem"),
  new Type(value = classOf[RussianlightstemTokenFilterBuilder], name = "russianlightstem"),
  new Type(value = classOf[EnglishpossessiveTokenFilterBuilder], name = "englishpossessive"),
  new Type(value = classOf[LowercaseTokenFilterBuilder], name = "lowercase"),
  new Type(value = classOf[HindinormalizationTokenFilterBuilder], name = "hindinormalization"),
  new Type(value = classOf[ScandinaviannormalizationTokenFilterBuilder], name = "scandinaviannormalization"),
  new Type(value = classOf[ThaiwordTokenFilterBuilder], name = "thaiword"),
  new Type(value = classOf[SynonymTokenFilterBuilder], name = "synonym"),
  new Type(value = classOf[IndonesianstemTokenFilterBuilder], name = "indonesianstem"),
  new Type(value = classOf[KeepwordTokenFilterBuilder], name = "keepword"),
  new Type(value = classOf[HyphenationcompoundwordTokenFilterBuilder], name = "hyphenationcompoundword"),
  new Type(value = classOf[DictionarycompoundwordTokenFilterBuilder], name = "dictionarycompoundword"),
  new Type(value = classOf[ItalianlightstemTokenFilterBuilder], name = "italianlightstem"),
  new Type(value = classOf[PatterncapturegroupTokenFilterBuilder], name = "patterncapturegroup"),
  new Type(value = classOf[ReverseStringTokenFilterBuilder], name = "reversestring")
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
final case class ShingleTokenFilterBuilder(@JsonProperty("min_shingle_size") minShingleSize: Integer, @JsonProperty("max_shingle_size") maxShingleSize: Integer, @JsonProperty("outputUnigrams") outputUnigrams: Boolean, @JsonProperty("OUIfNoShingles") outputUnigramsIfNoShingles: Boolean, @JsonProperty("tokenSeparator") tokenSeparator: String, @JsonProperty("fillerToken") fillerToken: String) extends TokenFilterBuilder[TokenFilterFactory]("shingle")
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
final case class TypeTokenFilterBuilder(@JsonProperty("types") types: String, @JsonProperty("useWhitelist") useWhitelist: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("type")
final case class GermanstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("germanstem")
final case class NGramTokenFilterBuilder(@JsonProperty("minGramSize") minGramSize: Integer, @JsonProperty("maxGramSize") maxGramSize: Integer) extends TokenFilterBuilder[TokenFilterFactory]("ngram")
final case class LimittokenpositionTokenFilterBuilder(@JsonProperty("maxTokenPosition") maxTokenPosition: Integer, @JsonProperty("consumeAllTokens") consumeAllTokens: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("limittokenposition")
final case class GreeklowercaseTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("greeklowercase")
final case class LimittokenoffsetTokenFilterBuilder(@JsonProperty("maxStartOffset") maxStartOffset: Integer, @JsonProperty("consumeAllTokens") consumeAllTokens: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("limittokenoffset")
final case class SnowballporterTokenFilterBuilder(@JsonProperty("protected") `protected` : String, @JsonProperty("language") language: String) extends TokenFilterBuilder[TokenFilterFactory]("snowballporter")
final case class TypeaspayloadTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("typeaspayload")
final case class PatternreplaceTokenFilterBuilder(@JsonProperty("pattern") pattern : String, @JsonProperty("replacement") replacement: String) extends TokenFilterBuilder[TokenFilterFactory]("patternreplace")
final case class CjkbigramTokenFilterBuilder(@JsonProperty("han") han: Boolean, @JsonProperty("hiragana") hiragana: Boolean, @JsonProperty("katakana") katakana: Boolean, @JsonProperty("hangul") hangul: Boolean, @JsonProperty("outputUnigrams") outputUnigrams: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("cjkbigram")
final case class KeywordmarkerTokenFilterBuilder(@JsonProperty("protected") `protected` : String, @JsonProperty("pattern") pattern: String, @JsonProperty("ignoreCase") ignoreCase: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("keywordmarker")
final case class SoranistemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("soranistem")
final case class ElisionTokenFilterBuilder(@JsonProperty("articles") articles: String, @JsonProperty("ignoreCase") ignoreCase: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("elision")
final case class HunspellstemTokenFilterBuilder(@JsonProperty("dictionary") dictionary: String, @JsonProperty("affix") affix: String, @JsonProperty("longestOnly") longestOnly: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("hunspellstem")
final case class CodepointcountTokenFilterBuilder(@JsonProperty("min") min: Integer, @JsonProperty("max") max: Integer) extends TokenFilterBuilder[TokenFilterFactory]("codepointcount")
final case class CzechstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("czechstem")
final case class TurkishlowercaseTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("turkishlowercase")
final case class DaterecognizerTokenFilterBuilder(@JsonProperty("datePattern") datePattern: String, @JsonProperty("locale") locale: String) extends TokenFilterBuilder[TokenFilterFactory]("daterecognizer")
final case class PortugueselightstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("portugueselightstem")
final case class IrishlowercaseTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("irishlowercase")
final case class CommongramsqueryTokenFilterBuilder( @JsonProperty("words") words: String, @JsonProperty("ignoreCase") ignoreCase: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("commongramsquery")
final case class NumericpayloadTokenFilterBuilder(@JsonProperty("payload") payload: Integer, @JsonProperty("typeMatch") typeMatch: String) extends TokenFilterBuilder[TokenFilterFactory]("numericpayload")
final case class ScandinavianfoldingTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("scandinavianfolding")
final case class GermannormalizationTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("germannormalization")
final case class DelimitedpayloadTokenFilterBuilder(@JsonProperty("encoder") encoder: String, @JsonProperty("delimiter") delimiter: String) extends TokenFilterBuilder[TokenFilterFactory]("delimitedpayload")
final case class WorddelimiterTokenFilterBuilder(@JsonProperty("protected") `protected` : String, @JsonProperty("preserveOriginal") preserveOriginal: Integer, @JsonProperty("splitOnNumerics") splitOnNumerics : Integer, @JsonProperty("splitOnCaseChange") splitOnCaseChange: Integer, @JsonProperty("catenateWords") catenateWords : Integer, @JsonProperty("catenateNumbers") catenateNumbers: Integer, @JsonProperty("catenateAll") catenateAll : Integer, @JsonProperty("generateWordParts") generateWordParts: Integer, @JsonProperty("stemEnglishPosse") stemEnglishPossessive : Integer, @JsonProperty("genNumberParts") generateNumberParts: Integer, @JsonProperty("types") types: String) extends TokenFilterBuilder[TokenFilterFactory]("worddelimiter")
final case class PortugueseminimalstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("portugueseminimalstem")
final case class RemoveduplicatesTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("removeduplicates")
final case class EdgengramTokenFilterBuilder(@JsonProperty("minGramSize") minGramSize: Integer, @JsonProperty("maxGramSize") maxGramSize: Integer) extends TokenFilterBuilder[TokenFilterFactory]("edgengram")
final case class LatvianstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("latvianstem")
final case class PorterstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("porterstem")
final case class FinnishlightstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("finnishlightstem")
final case class CommongramsTokenFilterBuilder(@JsonProperty("words") words: String, @JsonProperty("ignoreCase") ignoreCase: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("commongrams")
final case class GalicianstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("galicianstem")
final case class KstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("kstem")
final case class AsciifoldingTokenFilterBuilder(@JsonProperty("preserve_original") preserveOriginal:Boolean) extends TokenFilterBuilder[TokenFilterFactory]("asciifolding")
final case class NorwegianlightstemTokenFilterBuilder(@JsonProperty("variant") variant: String) extends TokenFilterBuilder[TokenFilterFactory]("norwegianlightstem")
final case class TrimTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("trim")
final case class LengthTokenFilterBuilder(@JsonProperty("min") min: Integer, @JsonProperty("max") max: Integer) extends TokenFilterBuilder[TokenFilterFactory]("length")
final case class DecimaldigitTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("decimaldigit")
final case class BrazilianstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("brazilianstem")
final case class CapitalizationTokenFilterBuilder(@JsonProperty("onlyFirstWord") onlyFirstWord:Boolean, @JsonProperty("keep") keep:String, @JsonProperty("keepIgnoreCase") keepIgnoreCase:Boolean, @JsonProperty("okPrefix") okPrefix:Boolean) extends TokenFilterBuilder[TokenFilterFactory]("capitalization")
final case class SerbiannormalizationTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("serbiannormalization")
final case class FrenchminimalstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("frenchminimalstem")
final case class EnglishminimalstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("englishminimalstem")
final case class LimittokencountTokenFilterBuilder(@JsonProperty("maxTokenCount") maxTokenCount: Integer, @JsonProperty("consumeAllTokens") consumeAllTokens:Boolean) extends TokenFilterBuilder[TokenFilterFactory]("limittokencount")
final case class HyphenatedwordsTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("hyphenatedwords")
final case class TruncateTokenFilterBuilder(@JsonProperty("prefixLength") prefixLength: Integer) extends TokenFilterBuilder[TokenFilterFactory]("truncate")
final case class TokenoffsetpayloadTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("tokenoffsetpayload")
final case class GalicianminimalstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("galicianminimalstem")
final case class RussianlightstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("russianlightstem")
final case class EnglishpossessiveTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("englishpossessive")
final case class LowercaseTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("lowercase")
final case class HindinormalizationTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("hindinormalization")
final case class ScandinaviannormalizationTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("scandinaviannormalization")
final case class ThaiwordTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("thaiword")
final case class SynonymTokenFilterBuilder(@JsonProperty("synonyms") synonyms: String, @JsonProperty("format") format: String, @JsonProperty("ignoreCase") ignoreCase: Boolean, @JsonProperty("expand") expand: Boolean, @JsonProperty("tokenizerFactory") tokenizerFactory: String) extends TokenFilterBuilder[TokenFilterFactory]("synonym")
final case class IndonesianstemTokenFilterBuilder(@JsonProperty("stemDerivational") stemDerivational: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("indonesianstem")
final case class KeepwordTokenFilterBuilder(@JsonProperty("words") words: String, @JsonProperty("ignoreCase") ignoreCase: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("keepword")
final case class HyphenationcompoundwordTokenFilterBuilder(@JsonProperty("hyphenator") hyphenator: String, @JsonProperty("encoding") encoding: String, @JsonProperty("dictionary") dictionary: String, @JsonProperty("minWordSize") minWordSize: Integer, @JsonProperty("minSubwordSize") minSubwordSize: Integer, @JsonProperty("maxSubwordSize") maxSubwordSize: Integer, @JsonProperty("onlyLongestMatch") onlyLongestMatch: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("hyphenationcompoundword")
final case class DictionarycompoundwordTokenFilterBuilder(@JsonProperty("dictionary") dictionary: String, @JsonProperty("minWordSize") minWordSize: Integer, @JsonProperty("minSubwordSize") minSubwordSize: Integer, @JsonProperty("maxSubwordSize") maxSubwordSize: Integer, @JsonProperty("onlyLongestMatch") onlyLongestMatch: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("dictionarycompoundword")
final case class ItalianlightstemTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("italianlightstem")
final case class PatterncapturegroupTokenFilterBuilder(@JsonProperty("pattern") pattern: String, @JsonProperty("preserve_original") preserve_original: Boolean) extends TokenFilterBuilder[TokenFilterFactory]("patterncapturegroup")
final case class ReverseStringTokenFilterBuilder() extends TokenFilterBuilder[TokenFilterFactory]("reversestring")


