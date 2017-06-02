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
package com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenizer;

import com.stratio.cassandra.lucene.common.JsonSerializer;
import org.junit.Test;
import java.io.IOException;
import static org.junit.Assert.*;


/**
 * @author Juan Pedro Gilaberte {@literal <jpgilaberte@stratio.com>}
 */
public class TokenizerTest {

    private <T> T assertAndTokenizer(String json, Class expectedClass) {
        try {
            Tokenizer abstractBuilder = JsonSerializer.fromString(json, Tokenizer.class);
            assertEquals("Expected " + expectedClass.getName() + " class", expectedClass, abstractBuilder.getClass());
            return (T) abstractBuilder;
        } catch (Exception e) {
            fail(e.getLocalizedMessage());
            return null;
        }
    }

    private void assertJsonParseFail(String json) throws IOException {
        JsonSerializer.fromString(json, Tokenizer.class);
    }

    private void assertExactValue(String paramName, Object expected, Object received) {
        assertEquals("Expected " +
                     paramName +
                     " equals to " +
                     expected.toString() +
                     " but received: " +
                     received.toString(), expected, received);
    }

    @Test
    public void testClassicTokenizerDefaultValues() {
        ClassicTokenizer builder = assertAndTokenizer("{type: \"classic\"}", ClassicTokenizer.class);
        assertExactValue("ClassicTokenizer.maxTokenLength",
                         ClassicTokenizer.DEFAULT_MAX_TOKEN_LENGTH,
                         builder.maxTokenLength);
    }

    @Test(expected = IOException.class)
    public void testClassicTokenizerInvalidParam() throws IOException {
        assertJsonParseFail("{type: \"classic\", max_toen_length: 250}");
    }

    @Test
    public void testKeywordTokenizerValidJSON() {
        String json = "{type: \"keyword\"}";
        KeywordTokenizer builder = assertAndTokenizer(json, KeywordTokenizer.class);
    }

    @Test
    public void testKeywordTokenizerDefaultValues() {
        KeywordTokenizer builder = assertAndTokenizer("{type: \"keyword\"}", KeywordTokenizer.class);
    }

    @Test
    public void testLetterTokenizerValidJSON() {
        assertAndTokenizer("{type: \"letter\"}", LetterTokenizer.class);
    }

    @Test
    public void testLowerCaseTokenizerValidJSON() {
        assertAndTokenizer("{type: \"lower_case\"}", LowerCaseTokenizer.class);
    }

    @Test
    public void testThaiTokenizerValidJSON() {
        assertAndTokenizer("{type: \"thai\"}", ThaiTokenizer.class);
    }

    @Test
    public void testNGramTokenizerValidJSON() {
        String json = "{type: \"ngram\", min_gram_size: 1, max_gram_size: 2}";
        NGramTokenizer builder = assertAndTokenizer(json, NGramTokenizer.class);
        assertExactValue("NGramTokenizer.min_gram_size", NGramTokenizer.DEFAULT_MIN_GRAM, builder.minGramSize);
        assertExactValue("NGramTokenizer.max_gram_size", NGramTokenizer.DEFAULT_MAX_GRAM, builder.maxGramSize);
    }

    @Test
    public void testNGramTokenizerDefaultValues() {
        String json = "{type: \"ngram\"}";
        NGramTokenizer builder = assertAndTokenizer(json, NGramTokenizer.class);
        assertExactValue("NGramTokenizer.min_gram_size", NGramTokenizer.DEFAULT_MIN_GRAM, builder.minGramSize);
        assertExactValue("NGramTokenizer.max_gram_size", NGramTokenizer.DEFAULT_MAX_GRAM, builder.maxGramSize);
    }

    @Test(expected = IOException.class)
    public void testNGramTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"ngram\", min_am: 1, max_gram: 1}");
    }

    @Test
    public void testEdgeNGramTokenizerValidJSON() {
        String json = "{type: \"edge_ngram\", min_gram_size: 1, max_gram_size: 2}";
        EdgeNGramTokenizer builder = assertAndTokenizer(json, EdgeNGramTokenizer.class);
        assertExactValue("EdgeNGramTokenizer.min_gram_size", EdgeNGramTokenizer.DEFAULT_MIN_GRAM, builder.minGramSize);
        assertExactValue("EdgeNGramTokenizer.max_gram_size", EdgeNGramTokenizer.DEFAULT_MAX_GRAM, builder.maxGramSize);
    }

    @Test
    public void testEdgeNGramTokenizerDefaultValues() {
        String json = "{type: \"edge_ngram\"}";
        EdgeNGramTokenizer builder = assertAndTokenizer(json, EdgeNGramTokenizer.class);
        assertExactValue("EdgeNGramTokenizer.min_gram", EdgeNGramTokenizer.DEFAULT_MIN_GRAM, builder.minGramSize);
        assertExactValue("EdgeNGramTokenizer.max_gram", EdgeNGramTokenizer.DEFAULT_MAX_GRAM, builder.maxGramSize);
    }

    @Test(expected = IOException.class)
    public void testEdgeNGramTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"edge_ngram\", min_am: 1, max_gram: 1}");
    }

    @Test
    public void testPathHierarchyTokenizerValidJSON() {
        String json = "{type: \"path_hierarchy\", reverse: false, delimiter: \"$\", replace: \"%\", skip: 3}";
        PathHierarchyTokenizer builder = assertAndTokenizer(json, PathHierarchyTokenizer.class);
        assertExactValue("PathHierarchyTokenizer.buffer_size", false, builder.reverse);
        assertExactValue("PathHierarchyTokenizer.delimiter", '$', builder.delimiter);
        assertExactValue("PathHierarchyTokenizer.replace", '%', builder.replace);
        assertExactValue("PathHierarchyTokenizer.skip", 3, builder.skip);
    }

    @Test
    public void testPathHierarchyTokenizerDefaultValues() {
        String json = "{type: \"path_hierarchy\"}";
        PathHierarchyTokenizer builder = assertAndTokenizer(json, PathHierarchyTokenizer.class);
        assertExactValue("PathHierarchyTokenizer.reverse",
                         PathHierarchyTokenizer.REVERSE,
                         builder.reverse);
        assertExactValue("PathHierarchyTokenizer.delimiter",
                         PathHierarchyTokenizer.DEFAULT_DELIMITER,
                         builder.delimiter);
        assertExactValue("PathHierarchyTokenizer.replace",
                         PathHierarchyTokenizer.DEFAULT_REPLACEMENT,
                         builder.replace);
        assertExactValue("PathHierarchyTokenizer.skip", PathHierarchyTokenizer.DEFAULT_SKIP, builder.skip);
    }

    @Test(expected = IOException.class)
    public void testPathHierarchyTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"path_hierarchy\", reverse: false, delimter: \"$\", replace: \"%\", skip: 3}");
    }

    @Test
    public void testPatternTokenizerValidJSON() {
        String json = "{type: \"pattern\", pattern: \"[a-z]\", group: 0}";
        PatternTokenizer builder = assertAndTokenizer(json, PatternTokenizer.class);
        assertExactValue("PathHierarchyTokenizer.pattern", "[a-z]", builder.pattern);
        assertExactValue("PathHierarchyTokenizer.group", 0, builder.group);
    }

    @Test
    public void testPatternTokenizerDefaultValues() {
        String json = "{type: \"pattern\"}";
        PatternTokenizer builder = assertAndTokenizer(json, PatternTokenizer.class);
        assertExactValue("PathHierarchyTokenizer.pattern", PatternTokenizer.DEFAULT_PATTERN, builder.pattern);
        assertExactValue("PathHierarchyTokenizer.group", PatternTokenizer.DEFAULT_GROUP, builder.group);
    }

    @Test(expected = IOException.class)
    public void testPatternTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"pattern\", paern: \"[a-z]\", group: 0}");
    }

    @Test
    public void testStandardTokenizerValidJSON() {
        String json = "{type: \"standard\", max_token_length: 246}";
        StandardTokenizer builder = assertAndTokenizer(json, StandardTokenizer.class);
        assertExactValue("StandardTokenizer.maxTokenLength", 246, builder.maxTokenLength);
    }

    @Test
    public void testStandardTokenizerDefaultValues() {
        StandardTokenizer builder = assertAndTokenizer("{type: \"standard\"}", StandardTokenizer.class);
        assertExactValue("ClassicTokenizer.maxTokenLength",
                         StandardTokenizer.DEFAULT_MAX_TOKEN_LENGTH,
                         builder.maxTokenLength);
    }

    @Test(expected = IOException.class)
    public void testStandardTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"standard\", max_token_ngth: 246}");
    }

    @Test
    public void testUAX29URLEmailTokenizerValidJSON() {
        String json = "{type: \"uax29_url_email\", max_token_length: 249}";
        UAX29URLEmailTokenizer builder = assertAndTokenizer(json, UAX29URLEmailTokenizer.class);
        assertExactValue("UAX29URLEmailTokenizer.maxTokenLength", 249, builder.maxTokenLength);
    }

    @Test
    public void testUAX29URLEmailTokenizerDefaultValues() {
        String json = "{type: \"uax29_url_email\"}";
        UAX29URLEmailTokenizer builder = assertAndTokenizer(json, UAX29URLEmailTokenizer.class);
        assertExactValue("UAX29URLEmailTokenizer.maxTokenLength",
                         UAX29URLEmailTokenizer.DEFAULT_MAX_TOKEN_LENGTH,
                         builder.maxTokenLength);
    }

    @Test(expected = IOException.class)
    public void testUAX29URLEmailTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"uax29_url_email\", max_token_lgth: 249}");
    }

    @Test
    public void testWhitespaceTokenizerValidJSON() {
        String json = "{type:\"whitespace\"}";
        assertAndTokenizer(json, WhitespaceTokenizer.class);
    }

//    @Test
//    public void testWikipediaTokenizerValidJSON() {
//        String json = "{type: \"wikipedia\", token_output: \"TOKENS_ONLY\", untokenized_types : [\"aaa\",\"bbb\"]}";
//        WikipediaTokenizer builder = assertAndTokenizer(json, WikipediaTokenizer.class);
//        assertExactValue("WikipediaTokenizer.token_output",
//                         WikipediaTokenizer.TokenOutputValue.TOKENS_ONLY,
//                         builder.tokenOutput);
//        assertExactValue("WikipediaTokenizer.untokenized_types",
//                         Sets.newHashSet("aaa", "bbb"),
//                         builder.untokenizedTypes);
//    }
//
//    @Test
//    public void testWikipediaTokenizerDefaultValues() {
//        String json = "{type: \"wikipedia\"}";
//        WikipediaTokenizer builder = assertAndTokenizer(json, WikipediaTokenizer.class);
//        assertExactValue("WikipediaTokenizer.token_output",
//                         WikipediaTokenizer.TokenOutputValue.TOKENS_ONLY,
//                         builder.tokenOutput);
//        assertExactValue("WikipediaTokenizer.untokenized_types", Sets.newHashSet(), builder.untokenizedTypes);
//    }
//
//    @Test(expected = IOException.class)
//    public void testWikipediaTokenizerInvalidJSON() throws IOException {
//        assertJsonParseFail("{type: \"wikipedia\", token_output: \"TOKENS_ONLY\", untoknized_types : [\"aaa\",\"bbb\"]}");
//    }

    @Test(expected = IOException.class)
    public void testInvalidTokenizerType() throws IOException {
        assertJsonParseFail("{type: \"invalid_type\"}");
    }
}