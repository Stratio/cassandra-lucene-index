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
package com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenFilter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.stratio.cassandra.lucene.builder.JSONBuilder;


/**
 * Created by jpgilaberte on 25/05/17.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ApostropheTokenFilter.class, name = "apostrophe"),
        @JsonSubTypes.Type(value = ArabicnormalizationTokenFilter.class, name = "arabicnormalization"),
        @JsonSubTypes.Type(value = ArabicstemTokenFilter.class, name = "arabicstem"),
        @JsonSubTypes.Type(value = BulgarianstemTokenFilter.class, name = "bulgarianstem"),
        @JsonSubTypes.Type(value = BrazilianstemTokenFilter.class, name = "brazilianstem"),
        @JsonSubTypes.Type(value = CjkbigramTokenFilter.class, name = "cjkbigram"),
        @JsonSubTypes.Type(value = CjkwidthTokenFilter.class, name = "cjkwidth"),
        @JsonSubTypes.Type(value = SoraninormalizationTokenFilter.class, name = "soraninormalization"),
        @JsonSubTypes.Type(value = SoranistemTokenFilter.class, name = "soranistem"),
        @JsonSubTypes.Type(value = CommongramsTokenFilter.class, name = "commongrams"),
        @JsonSubTypes.Type(value = CommongramsqueryTokenFilter.class, name = "commongramsquery"),
        @JsonSubTypes.Type(value = DictionarycompoundwordTokenFilter.class, name = "dictionarycompoundword"),
        @JsonSubTypes.Type(value = HyphenationcompoundwordTokenFilter.class, name = "hyphenationcompoundword"),
        @JsonSubTypes.Type(value = DecimaldigitTokenFilter.class, name = "decimaldigit"),
        @JsonSubTypes.Type(value = LowercaseTokenFilter.class, name = "lowercase"),
        @JsonSubTypes.Type(value = StopTokenFilter.class, name = "stop"),
        @JsonSubTypes.Type(value = TypeTokenFilter.class, name = "type"),
        @JsonSubTypes.Type(value = UppercaseTokenFilter.class, name = "uppercase"),
        @JsonSubTypes.Type(value = CzechstemTokenFilter.class, name = "czechstem"),
        @JsonSubTypes.Type(value = GermanlightstemTokenFilter.class, name = "germanlightstem"),
        @JsonSubTypes.Type(value = GermanminimalstemTokenFilter.class, name = "germanminimalstem"),
        @JsonSubTypes.Type(value = GermannormalizationTokenFilter.class, name = "germannormalization"),
        @JsonSubTypes.Type(value = GermanstemTokenFilter.class, name = "germanstem"),
        @JsonSubTypes.Type(value = GreeklowercaseTokenFilter.class, name = "greeklowercase"),
        @JsonSubTypes.Type(value = GreekstemTokenFilter.class, name = "greekstem"),
        @JsonSubTypes.Type(value = EnglishminimalstemTokenFilter.class, name = "englishminimalstem"),
        @JsonSubTypes.Type(value = EnglishpossessiveTokenFilter.class, name = "englishpossessive"),
        @JsonSubTypes.Type(value = KstemTokenFilter.class, name = "kstem"),
        @JsonSubTypes.Type(value = PorterstemTokenFilter.class, name = "porterstem"),
        @JsonSubTypes.Type(value = SpanishlightstemTokenFilter.class, name = "spanishlightstem"),
        @JsonSubTypes.Type(value = PersiannormalizationTokenFilter.class, name = "persiannormalization"),
        @JsonSubTypes.Type(value = FinnishlightstemTokenFilter.class, name = "finnishlightstem"),
        @JsonSubTypes.Type(value = FrenchlightstemTokenFilter.class, name = "frenchlightstem"),
        @JsonSubTypes.Type(value = FrenchminimalstemTokenFilter.class, name = "frenchminimalstem"),
        @JsonSubTypes.Type(value = IrishlowercaseTokenFilter.class, name = "irishlowercase"),
        @JsonSubTypes.Type(value = GalicianminimalstemTokenFilter.class, name = "galicianminimalstem"),
        @JsonSubTypes.Type(value = GalicianstemTokenFilter.class, name = "galicianstem"),
        @JsonSubTypes.Type(value = HindinormalizationTokenFilter.class, name = "hindinormalization"),
        @JsonSubTypes.Type(value = HindistemTokenFilter.class, name = "hindistem"),
        @JsonSubTypes.Type(value = HungarianlightstemTokenFilter.class, name = "hungarianlightstem"),
        @JsonSubTypes.Type(value = HunspellstemTokenFilter.class, name = "hunspellstem"),
        @JsonSubTypes.Type(value = IndonesianstemTokenFilter.class, name = "indonesianstem"),
        @JsonSubTypes.Type(value = IndicnormalizationTokenFilter.class, name = "indicnormalization"),
        @JsonSubTypes.Type(value = ItalianlightstemTokenFilter.class, name = "italianlightstem"),
        @JsonSubTypes.Type(value = LatvianstemTokenFilter.class, name = "latvianstem"),
        @JsonSubTypes.Type(value = AsciifoldingTokenFilter.class, name = "asciifolding"),
        @JsonSubTypes.Type(value = CapitalizationTokenFilter.class, name = "capitalization"),
        @JsonSubTypes.Type(value = CodepointcountTokenFilter.class, name = "codepointcount"),
        @JsonSubTypes.Type(value = DaterecognizerTokenFilter.class, name = "daterecognizer"),
        @JsonSubTypes.Type(value = FingerprintTokenFilter.class, name = "fingerprint"),
        @JsonSubTypes.Type(value = HyphenatedwordsTokenFilter.class, name = "hyphenatedwords"),
        @JsonSubTypes.Type(value = KeepwordTokenFilter.class, name = "keepword"),
        @JsonSubTypes.Type(value = KeywordmarkerTokenFilter.class, name = "keywordmarker"),
        @JsonSubTypes.Type(value = KeywordrepeatTokenFilter.class, name = "keywordrepeat"),
        @JsonSubTypes.Type(value = LengthTokenFilter.class, name = "length"),
        @JsonSubTypes.Type(value = LimittokencountTokenFilter.class, name = "limittokencount"),
        @JsonSubTypes.Type(value = LimittokenoffsetTokenFilter.class, name = "limittokenoffset"),
        @JsonSubTypes.Type(value = LimittokenpositionTokenFilter.class, name = "limittokenposition"),
        @JsonSubTypes.Type(value = RemoveduplicatesTokenFilter.class, name = "removeduplicates"),
        @JsonSubTypes.Type(value = StemmeroverrideTokenFilter.class, name = "stemmeroverride"),
        @JsonSubTypes.Type(value = TrimTokenFilter.class, name = "trim"),
        @JsonSubTypes.Type(value = TruncateTokenFilter.class, name = "truncate"),
        @JsonSubTypes.Type(value = WorddelimiterTokenFilter.class, name = "worddelimiter"),
        @JsonSubTypes.Type(value = ScandinavianfoldingTokenFilter.class, name = "scandinavianfolding"),
        @JsonSubTypes.Type(value = ScandinaviannormalizationTokenFilter.class, name = "scandinaviannormalization"),
        @JsonSubTypes.Type(value = EdgengramTokenFilter.class, name = "edgengram"),
        @JsonSubTypes.Type(value = NgramTokenFilter.class, name = "ngram"),
        @JsonSubTypes.Type(value = NorwegianlightstemTokenFilter.class, name = "norwegianlightstem"),
        @JsonSubTypes.Type(value = NorwegianminimalstemTokenFilter.class, name = "norwegianminimalstem"),
        @JsonSubTypes.Type(value = PatternreplaceTokenFilter.class, name = "patternreplace"),
        @JsonSubTypes.Type(value = PatterncapturegroupTokenFilter.class, name = "patterncapturegroup"),
        @JsonSubTypes.Type(value = DelimitedpayloadTokenFilter.class, name = "delimitedpayload"),
        @JsonSubTypes.Type(value = NumericpayloadTokenFilter.class, name = "numericpayload"),
        @JsonSubTypes.Type(value = TokenoffsetpayloadTokenFilter.class, name = "tokenoffsetpayload"),
        @JsonSubTypes.Type(value = TypeaspayloadTokenFilter.class, name = "typeaspayload"),
        @JsonSubTypes.Type(value = PortugueselightstemTokenFilter.class, name = "portugueselightstem"),
        @JsonSubTypes.Type(value = PortugueseminimalstemTokenFilter.class, name = "portugueseminimalstem"),
        @JsonSubTypes.Type(value = PortuguesestemTokenFilter.class, name = "portuguesestem"),
        @JsonSubTypes.Type(value = ReversestringTokenFilter.class, name = "reversestring"),
        @JsonSubTypes.Type(value = RussianlightstemTokenFilter.class, name = "russianlightstem"),
        @JsonSubTypes.Type(value = ShingleTokenFilter.class, name = "shingle"),
        @JsonSubTypes.Type(value = SnowballporterTokenFilter.class, name = "snowballporter"),
        @JsonSubTypes.Type(value = SerbiannormalizationTokenFilter.class, name = "serbiannormalization"),
        @JsonSubTypes.Type(value = ClassicTokenFilter.class, name = "classic"),
        @JsonSubTypes.Type(value = StandardTokenFilter.class, name = "standard"),
        @JsonSubTypes.Type(value = SwedishlightstemTokenFilter.class, name = "swedishlightstem"),
        @JsonSubTypes.Type(value = SynonymTokenFilter.class, name = "synonym"),
        @JsonSubTypes.Type(value = ThaiwordTokenFilter.class, name = "thaiword"),
        @JsonSubTypes.Type(value = TurkishlowercaseTokenFilter.class, name = "turkishlowercase"),
        @JsonSubTypes.Type(value = ElisionTokenFilter.class, name = "elision")
})
public class TokenFilter extends JSONBuilder{
}