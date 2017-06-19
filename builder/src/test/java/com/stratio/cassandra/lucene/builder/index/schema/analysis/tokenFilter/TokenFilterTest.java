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

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by jpgilaberte on 14/06/17.
 */
public class TokenFilterTest {


    @Test
    public void testApostropheTokenFilter() {
        String classExcepted = "ApostropheTokenFilter";
        String jsonExcepted = "{\"type\":\"apostrophe\"}";
        Object testObject = new ApostropheTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testArabicnormalizationTokenFilter() {
        String classExcepted = "ArabicnormalizationTokenFilter";
        String jsonExcepted = "{\"type\":\"arabicnormalization\"}";
        Object testObject = new ArabicnormalizationTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testArabicstemTokenFilter() {
        String classExcepted = "ArabicstemTokenFilter";
        String jsonExcepted = "{\"type\":\"arabicstem\"}";
        Object testObject = new ArabicstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testBulgarianstemTokenFilter() {
        String classExcepted = "BulgarianstemTokenFilter";
        String jsonExcepted = "{\"type\":\"bulgarianstem\"}";
        Object testObject = new BulgarianstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testBrazilianstemTokenFilter() {
        String classExcepted = "BrazilianstemTokenFilter";
        String jsonExcepted = "{\"type\":\"brazilianstem\"}";
        Object testObject = new BrazilianstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testCjkbigramTokenFilter() {
        String classExcepted = "CjkbigramTokenFilter";
        String jsonExcepted = "{\"type\":\"cjkbigram\",\"han\":true,\"hiragana\":true,\"katakana\":true,\"hangul\":true,\"outputUnigrams\":true}";
        Object testObject = new CjkbigramTokenFilter().setHan(true).setHangul(true).setHiragana(true).setKatakana(true).setOutputUnigrams(true);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testCjkwidthTokenFilter() {
        String classExcepted = "CjkwidthTokenFilter";
        String jsonExcepted = "{\"type\":\"cjkwidth\"}";
        Object testObject = new CjkwidthTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testSoraninormalizationTokenFilter() {
        String classExcepted = "SoraninormalizationTokenFilter";
        String jsonExcepted = "{\"type\":\"soraninormalization\"}";
        Object testObject = new SoraninormalizationTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testSoranistemTokenFilter() {
        String classExcepted = "SoranistemTokenFilter";
        String jsonExcepted = "{\"type\":\"soranistem\"}";
        Object testObject = new SoranistemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testCommongramsTokenFilter() {
        String classExcepted = "CommongramsTokenFilter";
        String jsonExcepted = "{\"type\":\"commongrams\",\"words\":\"words\",\"ignoreCase\":true}";
        Object testObject = new CommongramsTokenFilter().setIgnoreCase(true).setWords("words");
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testCommongramsqueryTokenFilter() {
        String classExcepted = "CommongramsqueryTokenFilter";
        String jsonExcepted = "{\"type\":\"commongramsquery\",\"words\":\"words\",\"ignoreCase\":true}";
        Object testObject = new CommongramsqueryTokenFilter().setWords("words").setIgnoreCase(true);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testDictionarycompoundwordTokenFilter() {
        String classExcepted = "DictionarycompoundwordTokenFilter";
        String jsonExcepted = "{\"type\":\"dictionarycompoundword\"}";
        Object testObject = new DictionarycompoundwordTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testHyphenationcompoundwordTokenFilter() {
        String classExcepted = "HyphenationcompoundwordTokenFilter";
        String jsonExcepted = "{\"type\":\"hyphenationcompoundword\"}";
        Object testObject = new HyphenationcompoundwordTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testDecimaldigitTokenFilter() {
        String classExcepted = "DecimaldigitTokenFilter";
        String jsonExcepted = "{\"type\":\"decimaldigit\"}";
        Object testObject = new DecimaldigitTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testLowercaseTokenFilter() {
        String classExcepted = "LowercaseTokenFilter";
        String jsonExcepted = "{\"type\":\"lowercase\"}";
        Object testObject = new LowercaseTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testStopTokenFilter() {
        String classExcepted = "StopTokenFilter";
        String jsonExcepted = "{\"type\":\"stop\"}";
        Object testObject = new StopTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testTypeTokenFilter() {
        String classExcepted = "TypeTokenFilter";
        String jsonExcepted = "{\"type\":\"type\"}";
        Object testObject = new TypeTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testUppercaseTokenFilter() {
        String classExcepted = "UppercaseTokenFilter";
        String jsonExcepted = "{\"type\":\"uppercase\"}";
        Object testObject = new UppercaseTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testCzechstemTokenFilter() {
        String classExcepted = "CzechstemTokenFilter";
        String jsonExcepted = "{\"type\":\"czechstem\"}";
        Object testObject = new CzechstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testGermanlightstemTokenFilter() {
        String classExcepted = "GermanlightstemTokenFilter";
        String jsonExcepted = "{\"type\":\"germanlightstem\"}";
        Object testObject = new GermanlightstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testGermanminimalstemTokenFilter() {
        String classExcepted = "GermanminimalstemTokenFilter";
        String jsonExcepted = "{\"type\":\"germanminimalstem\"}";
        Object testObject = new GermanminimalstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testGermannormalizationTokenFilter() {
        String classExcepted = "GermannormalizationTokenFilter";
        String jsonExcepted = "{\"type\":\"germannormalization\"}";
        Object testObject = new GermannormalizationTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testGermanstemTokenFilter() {
        String classExcepted = "GermanstemTokenFilter";
        String jsonExcepted = "{\"type\":\"germanstem\"}";
        Object testObject = new GermanstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testGreeklowercaseTokenFilter() {
        String classExcepted = "GreeklowercaseTokenFilter";
        String jsonExcepted = "{\"type\":\"greeklowercase\"}";
        Object testObject = new GreeklowercaseTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testGreekstemTokenFilter() {
        String classExcepted = "GreekstemTokenFilter";
        String jsonExcepted = "{\"type\":\"greekstem\"}";
        Object testObject = new GreekstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testEnglishminimalstemTokenFilter() {
        String classExcepted = "EnglishminimalstemTokenFilter";
        String jsonExcepted = "{\"type\":\"englishminimalstem\"}";
        Object testObject = new EnglishminimalstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testEnglishpossessiveTokenFilter() {
        String classExcepted = "EnglishpossessiveTokenFilter";
        String jsonExcepted = "{\"type\":\"englishpossessive\"}";
        Object testObject = new EnglishpossessiveTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testKstemTokenFilter() {
        String classExcepted = "KstemTokenFilter";
        String jsonExcepted = "{\"type\":\"kstem\"}";
        Object testObject = new KstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testPorterstemTokenFilter() {
        String classExcepted = "PorterstemTokenFilter";
        String jsonExcepted = "{\"type\":\"porterstem\"}";
        Object testObject = new PorterstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testSpanishlightstemTokenFilter() {
        String classExcepted = "SpanishlightstemTokenFilter";
        String jsonExcepted = "{\"type\":\"spanishlightstem\"}";
        Object testObject = new SpanishlightstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testPersiannormalizationTokenFilter() {
        String classExcepted = "PersiannormalizationTokenFilter";
        String jsonExcepted = "{\"type\":\"persiannormalization\"}";
        Object testObject = new PersiannormalizationTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testFinnishlightstemTokenFilter() {
        String classExcepted = "FinnishlightstemTokenFilter";
        String jsonExcepted = "{\"type\":\"finnishlightstem\"}";
        Object testObject = new FinnishlightstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testFrenchlightstemTokenFilter() {
        String classExcepted = "FrenchlightstemTokenFilter";
        String jsonExcepted = "{\"type\":\"frenchlightstem\"}";
        Object testObject = new FrenchlightstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testFrenchminimalstemTokenFilter() {
        String classExcepted = "FrenchminimalstemTokenFilter";
        String jsonExcepted = "{\"type\":\"frenchminimalstem\"}";
        Object testObject = new FrenchminimalstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testIrishlowercaseTokenFilter() {
        String classExcepted = "IrishlowercaseTokenFilter";
        String jsonExcepted = "{\"type\":\"irishlowercase\"}";
        Object testObject = new IrishlowercaseTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testGalicianminimalstemTokenFilter() {
        String classExcepted = "GalicianminimalstemTokenFilter";
        String jsonExcepted = "{\"type\":\"galicianminimalstem\"}";
        Object testObject = new GalicianminimalstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testGalicianstemTokenFilter() {
        String classExcepted = "GalicianstemTokenFilter";
        String jsonExcepted = "{\"type\":\"galicianstem\"}";
        Object testObject = new GalicianstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testHindinormalizationTokenFilter() {
        String classExcepted = "HindinormalizationTokenFilter";
        String jsonExcepted = "{\"type\":\"hindinormalization\"}";
        Object testObject = new HindinormalizationTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testHindistemTokenFilter() {
        String classExcepted = "HindistemTokenFilter";
        String jsonExcepted = "{\"type\":\"hindistem\"}";
        Object testObject = new HindistemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testHungarianlightstemTokenFilter() {
        String classExcepted = "HungarianlightstemTokenFilter";
        String jsonExcepted = "{\"type\":\"hungarianlightstem\"}";
        Object testObject = new HungarianlightstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testHunspellstemTokenFilter() {
        String classExcepted = "HunspellstemTokenFilter";
        String jsonExcepted = "{\"type\":\"hunspellstem\",\"dictionary\":\"path\",\"affix\":\"aff\"}";
        Object testObject = new HunspellstemTokenFilter().setAffix("aff").setDictionary("path").setLongestOnly(true);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testIndonesianstemTokenFilter() {
        String classExcepted = "IndonesianstemTokenFilter";
        String jsonExcepted = "{\"type\":\"indonesianstem\"}";
        Object testObject = new IndonesianstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testIndicnormalizationTokenFilter() {
        String classExcepted = "IndicnormalizationTokenFilter";
        String jsonExcepted = "{\"type\":\"indicnormalization\"}";
        Object testObject = new IndicnormalizationTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testItalianlightstemTokenFilter() {
        String classExcepted = "ItalianlightstemTokenFilter";
        String jsonExcepted = "{\"type\":\"italianlightstem\"}";
        Object testObject = new ItalianlightstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testLatvianstemTokenFilter() {
        String classExcepted = "LatvianstemTokenFilter";
        String jsonExcepted = "{\"type\":\"latvianstem\"}";
        Object testObject = new LatvianstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testAsciifoldingTokenFilter() {
        String classExcepted = "AsciifoldingTokenFilter";
        String jsonExcepted = "{\"type\":\"asciifolding\"}";
        Object testObject = new AsciifoldingTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testCapitalizationTokenFilter() {
        String classExcepted = "CapitalizationTokenFilter";
        String jsonExcepted = "{\"type\":\"capitalization\"}";
        Object testObject = new CapitalizationTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testCodepointcountTokenFilter() {
        String classExcepted = "CodepointcountTokenFilter";
        String jsonExcepted = "{\"type\":\"codepointcount\",\"min\":3,\"max\":5}";
        Object testObject = new CodepointcountTokenFilter().setMin(3).setMax(5);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testDaterecognizerTokenFilter() {
        String classExcepted = "DaterecognizerTokenFilter";
        String jsonExcepted = "{\"type\":\"daterecognizer\",\"datePattern\":\"pat\",\"locale\":\"locale\"}";
        Object testObject = new DaterecognizerTokenFilter().setDatePattern("pat").setLocale("locale");
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testFingerprintTokenFilter() {
        String classExcepted = "FingerprintTokenFilter";
        String jsonExcepted = "{\"type\":\"fingerprint\",\"maxOutputTokenSize\":1024,\"separator\":\" \"}";
        Object testObject = new FingerprintTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testHyphenatedwordsTokenFilter() {
        String classExcepted = "HyphenatedwordsTokenFilter";
        String jsonExcepted = "{\"type\":\"hyphenatedwords\"}";
        Object testObject = new HyphenatedwordsTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testKeepwordTokenFilter() {
        String classExcepted = "KeepwordTokenFilter";
        String jsonExcepted = "{\"type\":\"keepword\"}";
        Object testObject = new KeepwordTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testKeywordmarkerTokenFilter() {
        String classExcepted = "KeywordmarkerTokenFilter";
        String jsonExcepted = "{\"type\":\"keywordmarker\",\"pattern\":\"pat\",\"ignoreCase\":true,\"protected\":\"pro\"}";
        Object testObject = new KeywordmarkerTokenFilter().setProtect("pro").setPattern("pat").setIgnoreCase(true);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testKeywordrepeatTokenFilter() {
        String classExcepted = "KeywordrepeatTokenFilter";
        String jsonExcepted = "{\"type\":\"keywordrepeat\"}";
        Object testObject = new KeywordrepeatTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testLengthTokenFilter() {
        String classExcepted = "LengthTokenFilter";
        String jsonExcepted = "{\"type\":\"length\",\"min\":0,\"max\":10}";
        Object testObject = new LengthTokenFilter().setMax(10);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testLimittokencountTokenFilter() {
        String classExcepted = "LimittokencountTokenFilter";
        String jsonExcepted = "{\"type\":\"limittokencount\",\"maxTokenCount\":3,\"consumeAllTokens\":true}";
        Object testObject = new LimittokencountTokenFilter().setConsumeAllTokens(true).setMaxTokenCount(3);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testLimittokenoffsetTokenFilter() {
        String classExcepted = "LimittokenoffsetTokenFilter";
        String jsonExcepted = "{\"type\":\"limittokenoffset\"}";
        Object testObject = new LimittokenoffsetTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testLimittokenpositionTokenFilter() {
        String classExcepted = "LimittokenpositionTokenFilter";
        String jsonExcepted = "{\"type\":\"limittokenposition\"}";
        Object testObject = new LimittokenpositionTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testRemoveduplicatesTokenFilter() {
        String classExcepted = "RemoveduplicatesTokenFilter";
        String jsonExcepted = "{\"type\":\"removeduplicates\"}";
        Object testObject = new RemoveduplicatesTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testStemmeroverrideTokenFilter() {
        String classExcepted = "StemmeroverrideTokenFilter";
        String jsonExcepted = "{\"type\":\"stemmeroverride\"}";
        Object testObject = new StemmeroverrideTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testTrimTokenFilter() {
        String classExcepted = "TrimTokenFilter";
        String jsonExcepted = "{\"type\":\"trim\"}";
        Object testObject = new TrimTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testTruncateTokenFilter() {
        String classExcepted = "TruncateTokenFilter";
        String jsonExcepted = "{\"type\":\"truncate\",\"prefixLength\":2}";
        Object testObject = new TruncateTokenFilter().setPrefixLength(2);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testWorddelimiterTokenFilter() {
        String classExcepted = "WorddelimiterTokenFilter";
        String jsonExcepted = "{\"type\":\"worddelimiter\",\"splitOnNumerics\":1,\"splitOnCaseChange\":1,\"catenateWords\":0,\"catenateNumbers\":0,\"catenateAll\":0,\"generateWordParts\":0,\"genNumberParts\":1,\"stemEnglishPosse\":1}";
        Object testObject = new WorddelimiterTokenFilter().setCatenateAll(0).setGenerateWordParts(0);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testScandinavianfoldingTokenFilter() {
        String classExcepted = "ScandinavianfoldingTokenFilter";
        String jsonExcepted = "{\"type\":\"scandinavianfolding\"}";
        Object testObject = new ScandinavianfoldingTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testScandinaviannormalizationTokenFilter() {
        String classExcepted = "ScandinaviannormalizationTokenFilter";
        String jsonExcepted = "{\"type\":\"scandinaviannormalization\"}";
        Object testObject = new ScandinaviannormalizationTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testEdgengramTokenFilter() {
        String classExcepted = "EdgengramTokenFilter";
        String jsonExcepted = "{\"type\":\"edgengram\",\"minGramSize\":1,\"maxGramSize\":2}";
        Object testObject = new EdgengramTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testNgramTokenFilter() {
        String classExcepted = "NgramTokenFilter";
        String jsonExcepted = "{\"type\":\"ngram\",\"minGramSize\":1,\"maxGramSize\":2}";
        Object testObject = new NgramTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testNorwegianlightstemTokenFilter() {
        String classExcepted = "NorwegianlightstemTokenFilter";
        String jsonExcepted = "{\"type\":\"norwegianlightstem\",\"variant\":\"nn\"}";
        Object testObject = new NorwegianlightstemTokenFilter().setVariant("nn");
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testNorwegianminimalstemTokenFilter() {
        String classExcepted = "NorwegianminimalstemTokenFilter";
        String jsonExcepted = "{\"type\":\"norwegianminimalstem\"}";
        Object testObject = new NorwegianminimalstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testPatternreplaceTokenFilter() {
        String classExcepted = "PatternreplaceTokenFilter";
        String jsonExcepted = "{\"type\":\"patternreplace\"}";
        Object testObject = new PatternreplaceTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testPatterncapturegroupTokenFilter() {
        String classExcepted = "PatterncapturegroupTokenFilter";
        String jsonExcepted = "{\"type\":\"patterncapturegroup\"}";
        Object testObject = new PatterncapturegroupTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testDelimitedpayloadTokenFilter() {
        String classExcepted = "DelimitedpayloadTokenFilter";
        String jsonExcepted = "{\"type\":\"delimitedpayload\",\"encoder\":\"solr\",\"delimiter\":\"-\"}";
        Object testObject = new DelimitedpayloadTokenFilter().setDelimiter("-").setEncoder("solr");
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testNumericpayloadTokenFilter() {
        String classExcepted = "NumericpayloadTokenFilter";
        String jsonExcepted = "{\"type\":\"numericpayload\",\"payload\":12,\"typeMatch\":\"type\"}";
        Object testObject = new NumericpayloadTokenFilter().setPayload(12).setTypeMatch("type");
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testTokenoffsetpayloadTokenFilter() {
        String classExcepted = "TokenoffsetpayloadTokenFilter";
        String jsonExcepted = "{\"type\":\"tokenoffsetpayload\"}";
        Object testObject = new TokenoffsetpayloadTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testTypeaspayloadTokenFilter() {
        String classExcepted = "TypeaspayloadTokenFilter";
        String jsonExcepted = "{\"type\":\"typeaspayload\"}";
        Object testObject = new TypeaspayloadTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testPortugueselightstemTokenFilter() {
        String classExcepted = "PortugueselightstemTokenFilter";
        String jsonExcepted = "{\"type\":\"portugueselightstem\"}";
        Object testObject = new PortugueselightstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testPortugueseminimalstemTokenFilter() {
        String classExcepted = "PortugueseminimalstemTokenFilter";
        String jsonExcepted = "{\"type\":\"portugueseminimalstem\"}";
        Object testObject = new PortugueseminimalstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testPortuguesestemTokenFilter() {
        String classExcepted = "PortuguesestemTokenFilter";
        String jsonExcepted = "{\"type\":\"portuguesestem\"}";
        Object testObject = new PortuguesestemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testReversestringTokenFilter() {
        String classExcepted = "ReversestringTokenFilter";
        String jsonExcepted = "{\"type\":\"reversestring\"}";
        Object testObject = new ReversestringTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testRussianlightstemTokenFilter() {
        String classExcepted = "RussianlightstemTokenFilter";
        String jsonExcepted = "{\"type\":\"russianlightstem\"}";
        Object testObject = new RussianlightstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testShingleTokenFilter() {
        String classExcepted = "ShingleTokenFilter";
        String jsonExcepted = "{\"type\":\"shingle\",\"min_shingle_size\":2,\"max_shingle_size\":2,\"outputUnigrams\":false,\"ouifNoShingles\":false}";
        Object testObject = new ShingleTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testSnowballporterTokenFilter() {
        String classExcepted = "SnowballporterTokenFilter";
        String jsonExcepted = "{\"type\":\"snowballporter\"}";
        Object testObject = new SnowballporterTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testSerbiannormalizationTokenFilter() {
        String classExcepted = "SerbiannormalizationTokenFilter";
        String jsonExcepted = "{\"type\":\"serbiannormalization\"}";
        Object testObject = new SerbiannormalizationTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testClassicTokenFilter() {
        String classExcepted = "ClassicTokenFilter";
        String jsonExcepted = "{\"type\":\"classic\"}";
        Object testObject = new ClassicTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testStandardTokenFilter() {
        String classExcepted = "StandardTokenFilter";
        String jsonExcepted = "{\"type\":\"standard\"}";
        Object testObject = new StandardTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testSwedishlightstemTokenFilter() {
        String classExcepted = "SwedishlightstemTokenFilter";
        String jsonExcepted = "{\"type\":\"swedishlightstem\"}";
        Object testObject = new SwedishlightstemTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testSynonymTokenFilter() {
        String classExcepted = "SynonymTokenFilter";
        String jsonExcepted = "{\"type\":\"synonym\"}";
        Object testObject = new SynonymTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testThaiwordTokenFilter() {
        String classExcepted = "ThaiwordTokenFilter";
        String jsonExcepted = "{\"type\":\"thaiword\"}";
        Object testObject = new ThaiwordTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testTurkishlowercaseTokenFilter() {
        String classExcepted = "TurkishlowercaseTokenFilter";
        String jsonExcepted = "{\"type\":\"turkishlowercase\"}";
        Object testObject = new TurkishlowercaseTokenFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }


    @Test
    public void testElisionTokenFilter() {
        String classExcepted = "ElisionTokenFilter";
        String jsonExcepted = "{\"type\":\"elision\",\"articles\":\"path\",\"ignoreCase\":true}";
        Object testObject = new ElisionTokenFilter().setArticles("path").setIgnoreCase(true);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }

}
