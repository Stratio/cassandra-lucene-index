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

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.common.JsonSerializer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.util.TokenFilterFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


/** Tests for [[TokenFilterBuilder]].
  *
  * @author Juan Pedro Gilaberte `jpgilaberte@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class TokenFilterBuilderTest extends BaseScalaTest {

  test("available charFilter"){
    assert(TokenFilterFactory.availableTokenFilters().size() == 90)
  }

  type T = TokenFilterBuilder[TokenFilterFactory]

  def assertBuild(tokenizerBuilder: T, factoryClass: String, tokenizerClass: String) = {
    val tokenizerFactory = tokenizerBuilder.build
    val tokenizer = tokenizerFactory.create(new TokenStream() {override def incrementToken(): Boolean = true})
    assert(tokenizerFactory.getClass.getSimpleName == factoryClass)
    assert(tokenizer.getClass.getSimpleName == tokenizerClass)
  }

  def assertBuildException (tokenizerBuilder: T) = assertThrows[RuntimeException](tokenizerBuilder.build.create(new TokenStream() {override def incrementToken(): Boolean = true}))

  test("ApostropheFilterBuilder") {
    val jsonTest1 = """{"type":"apostrophe"}"""
    val factoryName = "ApostropheFilterFactory"
    val tokenizerName = "ApostropheFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ApostropheTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("ArabicnormalizationFilterBuilder") {
    val jsonTest1 = """{"type":"arabicnormalization"}"""
    val factoryName = "ArabicNormalizationFilterFactory"
    val tokenizerName = "ArabicNormalizationFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ArabicNormalizationTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("ArabicstemFilterBuilder") {
    val jsonTest1 = """{"type":"arabicstem"}"""
    val factoryName = "ArabicStemFilterFactory"
    val tokenizerName = "ArabicStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ArabicStemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("BulgarianstemFilterBuilder") {
    val jsonTest1 = """{"type":"bulgarianstem"}"""
    val factoryName = "BulgarianStemFilterFactory"
    val tokenizerName = "BulgarianStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[BulgarianstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("BrazilianstemFilterBuilder") {
    val jsonTest1 = """{"type":"brazilianstem"}"""
    val factoryName = "BrazilianStemFilterFactory"
    val tokenizerName = "BrazilianStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[BrazilianstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("CjkbigramFilterBuilder") {
    val jsonTest1 = """{"type":"cjkbigram"}"""
    val factoryName = "CJKBigramFilterFactory"
    val tokenizerName = "CJKBigramFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[CjkbigramTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("CjkwidthFilterBuilder") {
    val jsonTest1 = """{"type":"cjkwidth"}"""
    val factoryName = "CJKWidthFilterFactory"
    val tokenizerName = "CJKWidthFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[CjkwidthTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("SoraninormalizationFilterBuilder") {
    val jsonTest1 = """{"type":"soraninormalization"}"""
    val factoryName = "SoraniNormalizationFilterFactory"
    val tokenizerName = "SoraniNormalizationFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[SoraniNormalizationTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("SoranistemFilterBuilder") {
    val jsonTest1 = """{"type":"soranistem"}"""
    val factoryName = "SoraniStemFilterFactory"
    val tokenizerName = "SoraniStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[SoranistemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("CommongramsFilterBuilder") {
    val jsonTest1 = """{"type":"commongrams"}"""
    val factoryName = "CommonGramsFilterFactory"
    val tokenizerName = "CommonGramsFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[CommongramsTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("CommongramsqueryFilterBuilder") {
    val jsonTest1 = """{"type":"commongramsquery"}"""
    val factoryName = "CommonGramsQueryFilterFactory"
    val tokenizerName = "CommonGramsQueryFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[CommongramsqueryTokenFilterBuilder]), factoryName, tokenizerName)
  }
  //TODO: refactor test when object build it is not a filter (is a TokenStream)
  /*
  test("DictionarycompoundwordFilterBuilder") {
    val jsonTest1 = """{"type":"dictionarycompoundword", "dictionary":"path"}"""
    val factoryName = "DictionaryCompoundWordTokenFilterFactory"
    val tokenizerName = "DictionaryCompoundWordFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[DictionarycompoundwordTokenFilterBuilder]), factoryName, tokenizerName)
  }
  */
  test("HyphenationcompoundwordFilterBuilder") {
    val jsonTest1 = """{"type":"hyphenationcompoundword", "hyphenator":"h"}"""
    val factoryName = "HyphenationCompoundWordTokenFilterFactory"
    val tokenizerName = "HyphenationCompoundWordTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[HyphenationcompoundwordTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("DecimaldigitFilterBuilder") {
    val jsonTest1 = """{"type":"decimaldigit"}"""
    val factoryName = "DecimalDigitFilterFactory"
    val tokenizerName = "DecimalDigitFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[DecimaldigitTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("LowercaseFilterBuilder") {
    val jsonTest1 = """{"type":"lowercase"}"""
    val factoryName = "LowerCaseFilterFactory"
    val tokenizerName = "LowerCaseFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[LowercaseTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("StopFilterBuilder") {
    val jsonTest1 = """{"type":"stop"}"""
    val factoryName = "StopFilterFactory"
    val tokenizerName = "StopFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[StopTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("TypeFilterBuilder") {
    val jsonTest1 = """{"type":"type", "types":"types"}"""
    val factoryName = "TypeTokenFilterFactory"
    val tokenizerName = "TypeTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[TypeTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("UppercaseFilterBuilder") {
    val jsonTest1 = """{"type":"uppercase"}"""
    val factoryName = "UpperCaseFilterFactory"
    val tokenizerName = "UpperCaseFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[UpperCaseTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("CzechstemFilterBuilder") {
    val jsonTest1 = """{"type":"czechstem"}"""
    val factoryName = "CzechStemFilterFactory"
    val tokenizerName = "CzechStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[CzechstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("GermanlightstemFilterBuilder") {
    val jsonTest1 = """{"type":"germanlightstem"}"""
    val factoryName = "GermanLightStemFilterFactory"
    val tokenizerName = "GermanLightStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[GermanlightstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("GermanminimalstemFilterBuilder") {
    val jsonTest1 = """{"type":"germanminimalstem"}"""
    val factoryName = "GermanMinimalStemFilterFactory"
    val tokenizerName = "GermanMinimalStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[GermanMinimalStemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("GermannormalizationFilterBuilder") {
    val jsonTest1 = """{"type":"germannormalization"}"""
    val factoryName = "GermanNormalizationFilterFactory"
    val tokenizerName = "GermanNormalizationFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[GermannormalizationTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("GermanstemFilterBuilder") {
    val jsonTest1 = """{"type":"germanstem"}"""
    val factoryName = "GermanStemFilterFactory"
    val tokenizerName = "GermanStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[GermanstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("GreeklowercaseFilterBuilder") {
    val jsonTest1 = """{"type":"greeklowercase"}"""
    val factoryName = "GreekLowerCaseFilterFactory"
    val tokenizerName = "GreekLowerCaseFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[GreeklowercaseTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("GreekstemFilterBuilder") {
    val jsonTest1 = """{"type":"greekstem"}"""
    val factoryName = "GreekStemFilterFactory"
    val tokenizerName = "GreekStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[GreekstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("EnglishminimalstemFilterBuilder") {
    val jsonTest1 = """{"type":"englishminimalstem"}"""
    val factoryName = "EnglishMinimalStemFilterFactory"
    val tokenizerName = "EnglishMinimalStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[EnglishminimalstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("EnglishpossessiveFilterBuilder") {
    val jsonTest1 = """{"type":"englishpossessive"}"""
    val factoryName = "EnglishPossessiveFilterFactory"
    val tokenizerName = "EnglishPossessiveFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[EnglishpossessiveTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("KstemFilterBuilder") {
    val jsonTest1 = """{"type":"kstem"}"""
    val factoryName = "KStemFilterFactory"
    val tokenizerName = "KStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[KstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("PorterstemFilterBuilder") {
    val jsonTest1 = """{"type":"porterstem"}"""
    val factoryName = "PorterStemFilterFactory"
    val tokenizerName = "PorterStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PorterstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("SpanishlightstemFilterBuilder") {
    val jsonTest1 = """{"type":"spanishlightstem"}"""
    val factoryName = "SpanishLightStemFilterFactory"
    val tokenizerName = "SpanishLightStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[SpanishlightstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("PersiannormalizationFilterBuilder") {
    val jsonTest1 = """{"type":"persiannormalization"}"""
    val factoryName = "PersianNormalizationFilterFactory"
    val tokenizerName = "PersianNormalizationFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PersiannormalizationTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("FinnishlightstemFilterBuilder") {
    val jsonTest1 = """{"type":"finnishlightstem"}"""
    val factoryName = "FinnishLightStemFilterFactory"
    val tokenizerName = "FinnishLightStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[FinnishlightstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("FrenchlightstemFilterBuilder") {
    val jsonTest1 = """{"type":"frenchlightstem"}"""
    val factoryName = "FrenchLightStemFilterFactory"
    val tokenizerName = "FrenchLightStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[FrenchlightstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("FrenchminimalstemFilterBuilder") {
    val jsonTest1 = """{"type":"frenchminimalstem"}"""
    val factoryName = "FrenchMinimalStemFilterFactory"
    val tokenizerName = "FrenchMinimalStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[FrenchminimalstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("IrishlowercaseFilterBuilder") {
    val jsonTest1 = """{"type":"irishlowercase"}"""
    val factoryName = "IrishLowerCaseFilterFactory"
    val tokenizerName = "IrishLowerCaseFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[IrishlowercaseTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("GalicianminimalstemFilterBuilder") {
    val jsonTest1 = """{"type":"galicianminimalstem"}"""
    val factoryName = "GalicianMinimalStemFilterFactory"
    val tokenizerName = "GalicianMinimalStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[GalicianminimalstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("GalicianstemFilterBuilder") {
    val jsonTest1 = """{"type":"galicianstem"}"""
    val factoryName = "GalicianStemFilterFactory"
    val tokenizerName = "GalicianStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[GalicianstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("HindinormalizationFilterBuilder") {
    val jsonTest1 = """{"type":"hindinormalization"}"""
    val factoryName = "HindiNormalizationFilterFactory"
    val tokenizerName = "HindiNormalizationFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[HindinormalizationTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("HindistemFilterBuilder") {
    val jsonTest1 = """{"type":"hindistem"}"""
    val factoryName = "HindiStemFilterFactory"
    val tokenizerName = "HindiStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[HindistemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("HungarianlightstemFilterBuilder") {
    val jsonTest1 = """{"type":"hungarianlightstem"}"""
    val factoryName = "HungarianLightStemFilterFactory"
    val tokenizerName = "HungarianLightStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[HungarianlightstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  //TODO: refactor test. it is mandatory build filter with inform flow
  /*
  test("HunspellstemFilterBuilder") {
    val jsonTest1 = """{"type":"hunspellstem", "dictionary":"/tmp"}"""
    val factoryName = "HunspellStemFilterFactory"
    val tokenizerName = "HunspellStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[HunspellstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  */
  test("IndonesianstemFilterBuilder") {
    val jsonTest1 = """{"type":"indonesianstem"}"""
    val factoryName = "IndonesianStemFilterFactory"
    val tokenizerName = "IndonesianStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[IndonesianstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("IndicnormalizationFilterBuilder") {
    val jsonTest1 = """{"type":"indicnormalization"}"""
    val factoryName = "IndicNormalizationFilterFactory"
    val tokenizerName = "IndicNormalizationFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[IndicNormalizationTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("ItalianlightstemFilterBuilder") {
    val jsonTest1 = """{"type":"italianlightstem"}"""
    val factoryName = "ItalianLightStemFilterFactory"
    val tokenizerName = "ItalianLightStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ItalianlightstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("LatvianstemFilterBuilder") {
    val jsonTest1 = """{"type":"latvianstem"}"""
    val factoryName = "LatvianStemFilterFactory"
    val tokenizerName = "LatvianStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[LatvianstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("AsciifoldingFilterBuilder") {
    val jsonTest1 = """{"type":"asciifolding"}"""
    val factoryName = "ASCIIFoldingFilterFactory"
    val tokenizerName = "ASCIIFoldingFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[AsciifoldingTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("CapitalizationFilterBuilder") {
    val jsonTest1 = """{"type":"capitalization"}"""
    val factoryName = "CapitalizationFilterFactory"
    val tokenizerName = "CapitalizationFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[CapitalizationTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("CodepointcountFilterBuilder") {
    val jsonTest1 = """{"type":"codepointcount", "min":0, "max":1}"""
    val factoryName = "CodepointCountFilterFactory"
    val tokenizerName = "CodepointCountFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[CodepointcountTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("DaterecognizerFilterBuilder") {
    val jsonTest1 = """{"type":"daterecognizer"}"""
    val factoryName = "DateRecognizerFilterFactory"
    val tokenizerName = "DateRecognizerFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[DaterecognizerTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("FingerprintFilterBuilder") {
    val jsonTest1 = """{"type":"fingerprint"}"""
    val factoryName = "FingerprintFilterFactory"
    val tokenizerName = "FingerprintFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[FingerprintTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("HyphenatedwordsFilterBuilder") {
    val jsonTest1 = """{"type":"hyphenatedwords"}"""
    val factoryName = "HyphenatedWordsFilterFactory"
    val tokenizerName = "HyphenatedWordsFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[HyphenatedwordsTokenFilterBuilder]), factoryName, tokenizerName)
  }
  //TOOD: return non filter object
  /*
  test("KeepwordFilterBuilder") {
    val jsonTest1 = """{"type":"keepword"}"""
    val factoryName = "KeepWordFilterFactory"
    val tokenizerName = "KeepWordFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[KeepwordTokenFilterBuilder]), factoryName, tokenizerName)
  }
  */
  //TOOD: return non filter object
  /*
  test("KeywordmarkerFilterBuilder") {
    val jsonTest1 = """{"type":"keywordmarker"}"""
    val factoryName = "KeywordMarkerFilterFactory"
    val tokenizerName = "KeywordMarkerFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[KeywordmarkerTokenFilterBuilder]), factoryName, tokenizerName)
  }
  */
  test("KeywordrepeatFilterBuilder") {
    val jsonTest1 = """{"type":"keywordrepeat"}"""
    val factoryName = "KeywordRepeatFilterFactory"
    val tokenizerName = "KeywordRepeatFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[KeywordRepeatTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("LengthFilterBuilder") {
    val jsonTest1 = """{"type":"length", "min":"1", "max":"2"}"""
    val factoryName = "LengthFilterFactory"
    val tokenizerName = "LengthFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[LengthTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("LimittokencountFilterBuilder") {
    val jsonTest1 = """{"type":"limittokencount", "maxTokenCount":"3"}"""
    val factoryName = "LimitTokenCountFilterFactory"
    val tokenizerName = "LimitTokenCountFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[LimittokencountTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("LimittokenoffsetFilterBuilder") {
    val jsonTest1 = """{"type":"limittokenoffset", "maxStartOffset":"2"}"""
    val factoryName = "LimitTokenOffsetFilterFactory"
    val tokenizerName = "LimitTokenOffsetFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[LimittokenoffsetTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("LimittokenpositionFilterBuilder") {
    val jsonTest1 = """{"type":"limittokenposition", "maxTokenPosition":"2"}"""
    val factoryName = "LimitTokenPositionFilterFactory"
    val tokenizerName = "LimitTokenPositionFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[LimittokenpositionTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("RemoveduplicatesFilterBuilder") {
    val jsonTest1 = """{"type":"removeduplicates"}"""
    val factoryName = "RemoveDuplicatesTokenFilterFactory"
    val tokenizerName = "RemoveDuplicatesTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[RemoveduplicatesTokenFilterBuilder]), factoryName, tokenizerName)
  }
  //TOOD: return non filter object
  /*
  test("StemmeroverrideFilterBuilder") {
    val jsonTest1 = """{"type":"stemmeroverride"}"""
    val factoryName = "StemmerOverrideFilterFactory"
    val tokenizerName = "StemmerOverrideFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[StemmeroverrideTokenFilterBuilder]), factoryName, tokenizerName)
  }
  */
  test("TrimFilterBuilder") {
    val jsonTest1 = """{"type":"trim"}"""
    val factoryName = "TrimFilterFactory"
    val tokenizerName = "TrimFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[TrimTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("TruncateFilterBuilder") {
    val jsonTest1 = """{"type":"truncate"}"""
    val factoryName = "TruncateTokenFilterFactory"
    val tokenizerName = "TruncateTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[TruncateTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("WorddelimiterFilterBuilder") {
    val jsonTest1 = """{"type":"worddelimiter"}"""
    val factoryName = "WordDelimiterFilterFactory"
    val tokenizerName = "WordDelimiterFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[WorddelimiterTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("ScandinavianfoldingFilterBuilder") {
    val jsonTest1 = """{"type":"scandinavianfolding"}"""
    val factoryName = "ScandinavianFoldingFilterFactory"
    val tokenizerName = "ScandinavianFoldingFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ScandinavianfoldingTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("ScandinaviannormalizationFilterBuilder") {
    val jsonTest1 = """{"type":"scandinaviannormalization"}"""
    val factoryName = "ScandinavianNormalizationFilterFactory"
    val tokenizerName = "ScandinavianNormalizationFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ScandinaviannormalizationTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("EdgengramFilterBuilder") {
    val jsonTest1 = """{"type":"edgengram"}"""
    val factoryName = "EdgeNGramFilterFactory"
    val tokenizerName = "EdgeNGramTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[EdgengramTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("NgramFilterBuilder") {
    val jsonTest1 = """{"type":"ngram"}"""
    val factoryName = "NGramFilterFactory"
    val tokenizerName = "NGramTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[NGramTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("NorwegianlightstemFilterBuilder") {
    val jsonTest1 = """{"type":"norwegianlightstem"}"""
    val factoryName = "NorwegianLightStemFilterFactory"
    val tokenizerName = "NorwegianLightStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[NorwegianlightstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("NorwegianminimalstemFilterBuilder") {
    val jsonTest1 = """{"type":"norwegianminimalstem"}"""
    val factoryName = "NorwegianMinimalStemFilterFactory"
    val tokenizerName = "NorwegianMinimalStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[NorwegianminimalstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("PatternreplaceFilterBuilder") {
    val jsonTest1 = """{"type":"patternreplace", "pattern":"pattern"}"""
    val factoryName = "PatternReplaceFilterFactory"
    val tokenizerName = "PatternReplaceFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PatternreplaceTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("PatterncapturegroupFilterBuilder") {
    val jsonTest1 = """{"type":"patterncapturegroup", "pattern":"pattern"}"""
    val factoryName = "PatternCaptureGroupFilterFactory"
    val tokenizerName = "PatternCaptureGroupTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PatterncapturegroupTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("DelimitedpayloadFilterBuilder") {
    val jsonTest1 = """{"type":"delimitedpayload", "encoder":"UTF-8"}"""
    val factoryName = "DelimitedPayloadTokenFilterFactory"
    val tokenizerName = "DelimitedPayloadTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[DelimitedpayloadTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("NumericpayloadFilterBuilder") {
    val jsonTest1 = """{"type":"numericpayload", "payload":1, "typeMatch":"type"}"""
    val factoryName = "NumericPayloadTokenFilterFactory"
    val tokenizerName = "NumericPayloadTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[NumericpayloadTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("TokenoffsetpayloadFilterBuilder") {
    val jsonTest1 = """{"type":"tokenoffsetpayload"}"""
    val factoryName = "TokenOffsetPayloadTokenFilterFactory"
    val tokenizerName = "TokenOffsetPayloadTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[TokenoffsetpayloadTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("TypeaspayloadFilterBuilder") {
    val jsonTest1 = """{"type":"typeaspayload"}"""
    val factoryName = "TypeAsPayloadTokenFilterFactory"
    val tokenizerName = "TypeAsPayloadTokenFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[TypeaspayloadTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("PortugueselightstemFilterBuilder") {
    val jsonTest1 = """{"type":"portugueselightstem"}"""
    val factoryName = "PortugueseLightStemFilterFactory"
    val tokenizerName = "PortugueseLightStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PortugueselightstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("PortugueseminimalstemFilterBuilder") {
    val jsonTest1 = """{"type":"portugueseminimalstem"}"""
    val factoryName = "PortugueseMinimalStemFilterFactory"
    val tokenizerName = "PortugueseMinimalStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PortugueseminimalstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("PortuguesestemFilterBuilder") {
    val jsonTest1 = """{"type":"portuguesestem"}"""
    val factoryName = "PortugueseStemFilterFactory"
    val tokenizerName = "PortugueseStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PortugueseStemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("ReversestringFilterBuilder") {
    val jsonTest1 = """{"type":"reversestring"}"""
    val factoryName = "ReverseStringFilterFactory"
    val tokenizerName = "ReverseStringFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ReverseStringTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("RussianlightstemFilterBuilder") {
    val jsonTest1 = """{"type":"russianlightstem"}"""
    val factoryName = "RussianLightStemFilterFactory"
    val tokenizerName = "RussianLightStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[RussianlightstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("ShingleFilterBuilder") {
    val jsonTest1 = """{"type":"shingle"}"""
    val factoryName = "ShingleFilterFactory"
    val tokenizerName = "ShingleFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ShingleTokenFilterBuilder]), factoryName, tokenizerName)
  }
  /*
  test("SnowballporterFilterBuilder") {
    val jsonTest1 = """{"type":"snowballporter"}"""
    val factoryName = "SnowballPorterFilterFactory"
    val tokenizerName = "SnowballPorterFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[SnowballporterTokenFilterBuilder]), factoryName, tokenizerName)
  }
  */
  test("SerbiannormalizationFilterBuilder") {
    val jsonTest1 = """{"type":"serbiannormalization"}"""
    val factoryName = "SerbianNormalizationFilterFactory"
    val tokenizerName = "SerbianNormalizationFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[SerbiannormalizationTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("ClassicFilterBuilder") {
    val jsonTest1 = """{"type":"classic"}"""
    val factoryName = "ClassicFilterFactory"
    val tokenizerName = "ClassicFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ClassicTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("StandardFilterBuilder") {
    val jsonTest1 = """{"type":"standard"}"""
    val factoryName = "StandardFilterFactory"
    val tokenizerName = "StandardFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[StandardTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("SwedishlightstemFilterBuilder") {
    val jsonTest1 = """{"type":"swedishlightstem"}"""
    val factoryName = "SwedishLightStemFilterFactory"
    val tokenizerName = "SwedishLightStemFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[SwedishlightstemTokenFilterBuilder]), factoryName, tokenizerName)
  }
  //TODO: refactor test. it is mandatory build filter with inform flow
  /*
  test("SynonymFilterBuilder") {
    val jsonTest1 = """{"type":"synonym", "synonyms":"path"}"""
    val factoryName = "SynonymFilterFactory"
    val tokenizerName = "SynonymFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[SynonymTokenFilterBuilder]), factoryName, tokenizerName)
  }
  */
  test("ThaiwordFilterBuilder") {
    val jsonTest1 = """{"type":"thaiword"}"""
    val factoryName = "ThaiWordFilterFactory"
    val tokenizerName = "ThaiWordFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ThaiwordTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("TurkishlowercaseFilterBuilder") {
    val jsonTest1 = """{"type":"turkishlowercase"}"""
    val factoryName = "TurkishLowerCaseFilterFactory"
    val tokenizerName = "TurkishLowerCaseFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[TurkishlowercaseTokenFilterBuilder]), factoryName, tokenizerName)
  }
  test("ElisionFilterBuilder") {
    val jsonTest1 = """{"type":"elision"}"""
    val factoryName = "ElisionFilterFactory"
    val tokenizerName = "ElisionFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ElisionTokenFilterBuilder]), factoryName, tokenizerName)
  }
}
