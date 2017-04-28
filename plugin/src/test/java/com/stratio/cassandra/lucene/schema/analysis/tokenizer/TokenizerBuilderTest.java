package com.stratio.cassandra.lucene.schema.analysis.tokenizer;

import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.common.JsonSerializer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.th.ThaiTokenizer;
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class TokenizerBuilderTest {

    private <T> T assertBuilderAndTokenizer(String json, Class expectedBuilderClass, Class expectedTokenizerClass) {
        try {
            TokenizerBuilder abstractBuilder = JsonSerializer.fromString(json, TokenizerBuilder.class);
            assertEquals("Expected " + expectedBuilderClass.getName() + " class",
                         expectedBuilderClass,
                         abstractBuilder.getClass());
            Tokenizer tokenizer = abstractBuilder.buildTokenizer();
            assertEquals("Expected " + expectedTokenizerClass.getName() + " class",
                         expectedTokenizerClass,
                         tokenizer.getClass());
            return (T) abstractBuilder;
        } catch (Exception e) {
            fail(e.getLocalizedMessage());
            return null;
        }
    }

    private void assertJsonParseFail(String json) throws IOException {
        JsonSerializer.fromString(json, TokenizerBuilder.class);
    }

    private void assertJsonParseFail(String json, String message) {
        try {
            JsonSerializer.fromString(json, TokenizerBuilder.class);
        } catch (IOException e) {
            assertEquals("Expected IOException with message: " +
                         message +
                         " but received: " +
                         e.getMessage() +
                         " localMess: " +
                         e.getLocalizedMessage(), message, e.getMessage());
        }
        assertFalse("Parsing: " + json + " must generate an IOException with message: " + message + " but does not.",
                    true);
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
    public void testClassicTokenizerValidJSON() {
        String json = "{type: \"classic\", max_token_length: 250}";
        ClassicTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                    ClassicTokenizerBuilder.class,
                                                                    ClassicTokenizer.class);
        assertExactValue("ClassicTokenizerBuilder.maxTokenLength", 250, builder.maxTokenLength);
    }

    @Test
    public void testClassicTokenizerDefaultValues() {
        ClassicTokenizerBuilder builder = assertBuilderAndTokenizer("{type: \"classic\"}",
                                                                    ClassicTokenizerBuilder.class,
                                                                    ClassicTokenizer.class);
        assertExactValue("ClassicTokenizerBuilder.maxTokenLength",
                         ClassicTokenizerBuilder.DEFAULT_MAX_TOKEN_LENGTH,
                         builder.maxTokenLength);
    }

    @Test(expected = IOException.class)
    public void testClassicTokenizerInvalidParam() throws IOException {
        assertJsonParseFail("{type: \"classic\", max_toen_length: 250}");
    }

    @Test
    public void testKeywordTokenizerValidJSON() {
        String json = "{type: \"keyword\", buffer_size: 256}";
        KeywordTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                    KeywordTokenizerBuilder.class,
                                                                    KeywordTokenizer.class);
        assertExactValue("KeywordTokenizer.bufferSize", 256, builder.bufferSize);
    }

    @Test
    public void testKeywordTokenizerDefaultValues() {
        KeywordTokenizerBuilder builder = assertBuilderAndTokenizer("{type: \"keyword\"}",
                                                                    KeywordTokenizerBuilder.class,
                                                                    KeywordTokenizer.class);
        assertExactValue("ClassicTokenizerBuilder.maxTokenLength",
                         KeywordTokenizerBuilder.DEFAULT_BUFFER_SIZE,
                         builder.bufferSize);
    }

    @Test(expected = IOException.class)
    public void testKeywordTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"keyword\", bufer_size: 256}");
    }

    @Test
    public void testLetterTokenizerValidJSON() {
        assertBuilderAndTokenizer("{type: \"letter\"}", LetterTokenizerBuilder.class, LetterTokenizer.class);
    }

    @Test
    public void testLowerCaseTokenizerValidJSON() {
        assertBuilderAndTokenizer("{type: \"lower_case\"}", LowerCaseTokenizerBuilder.class, LowerCaseTokenizer.class);
    }

    @Test
    public void testThaiTokenizerValidJSON() {
        assertBuilderAndTokenizer("{type: \"thai\"}", ThaiTokenizerBuilder.class, ThaiTokenizer.class);
    }

    @Test
    public void testNGramTokenizerValidJSON() {
        String json = "{type: \"ngram\", min_gram: 1, max_gram: 2}";
        NGramTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                  NGramTokenizerBuilder.class,
                                                                  NGramTokenizer.class);
        assertExactValue("NGramTokenizerBuilder.min_gram", NGramTokenizerBuilder.DEFAULT_MIN_GRAM, builder.minGram);
        assertExactValue("NGramTokenizerBuilder.max_gram", NGramTokenizerBuilder.DEFAULT_MAX_GRAM, builder.maxGram);
    }

    @Test
    public void testNGramTokenizerDefaultValues() {
        String json = "{type: \"ngram\"}";
        NGramTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                  NGramTokenizerBuilder.class,
                                                                  NGramTokenizer.class);
        assertExactValue("NGramTokenizerBuilder.min_gram", NGramTokenizerBuilder.DEFAULT_MIN_GRAM, builder.minGram);
        assertExactValue("NGramTokenizerBuilder.max_gram", NGramTokenizerBuilder.DEFAULT_MAX_GRAM, builder.maxGram);
    }

    @Test(expected = IOException.class)
    public void testNGramTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"ngram\", min_am: 1, max_gram: 1}");
    }

    @Test
    public void testEdgeNGramTokenizerValidJSON() {
        String json = "{type: \"edge_ngram\", min_gram: 1, max_gram: 1}";
        EdgeNGramTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                      EdgeNGramTokenizerBuilder.class,
                                                                      EdgeNGramTokenizer.class);
        assertExactValue("EdgeNGramTokenizerBuilder.min_gram",
                         EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE,
                         builder.minGram);
        assertExactValue("EdgeNGramTokenizerBuilder.max_gram",
                         EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE,
                         builder.maxGram);
    }

    @Test
    public void testEdgeNGramTokenizerDefaultValues() {
        String json = "{type: \"edge_ngram\"}";
        EdgeNGramTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                      EdgeNGramTokenizerBuilder.class,
                                                                      EdgeNGramTokenizer.class);
        assertExactValue("EdgeNGramTokenizerBuilder.min_gram",
                         EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE,
                         builder.minGram);
        assertExactValue("EdgeNGramTokenizerBuilder.max_gram",
                         EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE,
                         builder.maxGram);
    }

    @Test(expected = IOException.class)
    public void testEdgeNGramTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"edge_ngram\", min_am: 1, max_gram: 1}");
    }

    @Test
    public void testPathHierarchyTokenizerValidJSON() {
        String json = "{type: \"path_hierarchy\", buffer_size: 246, delimiter: \"$\", replacement: \"%\", skip: 3}";
        PathHierarchyTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                          PathHierarchyTokenizerBuilder.class,
                                                                          PathHierarchyTokenizer.class);
        assertExactValue("PathHierarchyTokenizerBuilder.buffer_size", 246, builder.bufferSize);
        assertExactValue("PathHierarchyTokenizerBuilder.delimiter", '$', builder.delimiter);
        assertExactValue("PathHierarchyTokenizerBuilder.replacement", '%', builder.replacement);
        assertExactValue("PathHierarchyTokenizerBuilder.skip", 3, builder.skip);
    }

    @Test
    public void testPathHierarchyTokenizerDefaultValues() {
        String json = "{type: \"path_hierarchy\"}";
        PathHierarchyTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                          PathHierarchyTokenizerBuilder.class,
                                                                          PathHierarchyTokenizer.class);
        assertExactValue("PathHierarchyTokenizerBuilder.buffer_size",
                         PathHierarchyTokenizerBuilder.DEFAULT_BUFFER_SIZE,
                         builder.bufferSize);
        assertExactValue("PathHierarchyTokenizerBuilder.delimiter",
                         PathHierarchyTokenizerBuilder.DEFAULT_DELIMITER,
                         builder.delimiter);
        assertExactValue("PathHierarchyTokenizerBuilder.replacement",
                         PathHierarchyTokenizerBuilder.DEFAULT_REPLACEMENT,
                         builder.replacement);
        assertExactValue("PathHierarchyTokenizerBuilder.skip",
                         PathHierarchyTokenizerBuilder.DEFAULT_SKIP,
                         builder.skip);
    }

    @Test(expected = IOException.class)
    public void testPathHierarchyTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"path_hierarchy\", buffer_size: 246, delimter: \"$\", replacement: \"%\", skip: 3}");
    }

    @Test
    public void testPatternTokenizerValidJSON() {
        String json = "{type: \"pattern\", pattern: \"[a-z]\", flags: 35, group: 0}";
        PatternTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                    PatternTokenizerBuilder.class,
                                                                    PatternTokenizer.class);
        assertExactValue("PathHierarchyTokenizerBuilder.pattern", "[a-z]", builder.pattern);
        assertExactValue("PathHierarchyTokenizerBuilder.flags", 35, builder.flags);
        assertExactValue("PathHierarchyTokenizerBuilder.group", 0, builder.group);
    }

    @Test
    public void testPatternTokenizerDefaultValues() {
        String json = "{type: \"pattern\"}";
        PatternTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                    PatternTokenizerBuilder.class,
                                                                    PatternTokenizer.class);
        assertExactValue("PathHierarchyTokenizerBuilder.pattern",
                         PatternTokenizerBuilder.DEFAULT_PATTERN,
                         builder.pattern);
        assertExactValue("PathHierarchyTokenizerBuilder.group", PatternTokenizerBuilder.DEFAULT_GROUP, builder.group);
        assertExactValue("PathHierarchyTokenizerBuilder.group", PatternTokenizerBuilder.DEFAULT_FLAGS, builder.flags);
    }

    @Test(expected = IOException.class)
    public void testPatternTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"pattern\", paern: \"[a-z]\", flags: 35, group: 0}");
    }

    @Test
    public void testReversePathHierarchyTokenizerValidJSON() {
        String
                json
                = "{type: \"reverse_path_hierarchy\", buffer_size: 246, delimiter: \"/\", replacement: \"%\", skip: 3}";
        ReversePathHierarchyTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                                 ReversePathHierarchyTokenizerBuilder.class,
                                                                                 ReversePathHierarchyTokenizer.class);
        assertExactValue("ReversePathHierarchyTokenizerBuilder.buffer_size", 246, builder.bufferSize);
        assertExactValue("ReversePathHierarchyTokenizerBuilder.delimiter", '/', builder.delimiter);
        assertExactValue("ReversePathHierarchyTokenizerBuilder.replacement", '%', builder.replacement);
        assertExactValue("ReversePathHierarchyTokenizerBuilder.skip", 3, builder.skip);
    }

    @Test
    public void testReversePathHierarchyTokenizerDefaultValues() {
        String json = "{type: \"reverse_path_hierarchy\"}";
        ReversePathHierarchyTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                                 ReversePathHierarchyTokenizerBuilder.class,
                                                                                 ReversePathHierarchyTokenizer.class);
        assertExactValue("PathHierarchyTokenizerBuilder.buffer_size",
                         ReversePathHierarchyTokenizerBuilder.DEFAULT_BUFFER_SIZE,
                         builder.bufferSize);
        assertExactValue("PathHierarchyTokenizerBuilder.delimiter",
                         ReversePathHierarchyTokenizerBuilder.DEFAULT_DELIMITER,
                         builder.delimiter);
        assertExactValue("PathHierarchyTokenizerBuilder.replacement",
                         ReversePathHierarchyTokenizerBuilder.DEFAULT_REPLACEMENT,
                         builder.replacement);
        assertExactValue("PathHierarchyTokenizerBuilder.skip",
                         ReversePathHierarchyTokenizerBuilder.DEFAULT_SKIP,
                         builder.skip);
    }

    @Test(expected = IOException.class)
    public void testReversePathHierarchyTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail(
                "{type: \"reverse_path_hierarchy\", buffer_size: 246, delimiter: \"/\", replacent: \"%\", skip: 3}");
    }

    @Test
    public void testStandardTokenizerValidJSON() {
        String json = "{type: \"standard\", max_token_length: 246}";
        StandardTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                     StandardTokenizerBuilder.class,
                                                                     StandardTokenizer.class);
        assertExactValue("StandardTokenizerBuilder.maxTokenLength", 246, builder.maxTokenLength);
    }

    @Test
    public void testStandardTokenizerDefaultValues() {
        StandardTokenizerBuilder builder = assertBuilderAndTokenizer("{type: \"standard\"}",
                                                                     StandardTokenizerBuilder.class,
                                                                     StandardTokenizer.class);
        assertExactValue("ClassicTokenizerBuilder.maxTokenLength",
                         StandardTokenizerBuilder.DEFAULT_MAX_TOKEN_LENGTH,
                         builder.maxTokenLength);
    }

    @Test(expected = IOException.class)
    public void testStandardTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"standard\", max_token_ngth: 246}");
    }

    @Test
    public void testUAX29URLEmailTokenizerValidJSON() {
        String json = "{type: \"uax29_url_email\", max_token_length: 249}";
        UAX29URLEmailTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                          UAX29URLEmailTokenizerBuilder.class,
                                                                          UAX29URLEmailTokenizer.class);
        assertExactValue("UAX29URLEmailTokenizerBuilder.maxTokenLength", 249, builder.maxTokenLength);
    }

    @Test
    public void testUAX29URLEmailTokenizerDefaultValues() {
        String json = "{type: \"uax29_url_email\"}";
        UAX29URLEmailTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                          UAX29URLEmailTokenizerBuilder.class,
                                                                          UAX29URLEmailTokenizer.class);
        assertExactValue("UAX29URLEmailTokenizerBuilder.maxTokenLength",
                         UAX29URLEmailTokenizerBuilder.DEFAULT_MAX_TOKEN_LENGTH,
                         builder.maxTokenLength);
    }

    @Test(expected = IOException.class)
    public void testUAX29URLEmailTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"uax29_url_email\", max_token_lgth: 249}");
    }

    @Test
    public void testUnicodeWhitespaceTokenizerValidJSON() {
        String json = "{type:\"unicode_whitespace\"}";
        assertBuilderAndTokenizer(json, UnicodeWhitespaceTokenizerBuilder.class, UnicodeWhitespaceTokenizer.class);
    }

    @Test
    public void testWhitespaceTokenizerValidJSON() {
        String json = "{type:\"whitespace\"}";
        assertBuilderAndTokenizer(json, WhitespaceTokenizerBuilder.class, WhitespaceTokenizer.class);
    }

    @Test
    public void testWikipediaTokenizerValidJSON() {
        String json = "{type: \"wikipedia\", token_output: \"TOKENS_ONLY\", untokenized_types : [\"aaa\",\"bbb\"]}";
        WikipediaTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                      WikipediaTokenizerBuilder.class,
                                                                      WikipediaTokenizer.class);
        assertExactValue("WikipediaTokenizerBuilder.token_output",
                         WikipediaTokenizerBuilder.TokenOutputValue.TOKENS_ONLY,
                         builder.tokenOutput);
        assertExactValue("WikipediaTokenizerBuilder.untokenized_types",
                         Sets.newHashSet("aaa", "bbb"),
                         builder.untokenizedTypes);
    }

    @Test
    public void testWikipediaTokenizerDefaultValues() {
        String json = "{type: \"wikipedia\"}";
        WikipediaTokenizerBuilder builder = assertBuilderAndTokenizer(json,
                                                                      WikipediaTokenizerBuilder.class,
                                                                      WikipediaTokenizer.class);
        assertExactValue("WikipediaTokenizerBuilder.token_output",
                         WikipediaTokenizerBuilder.TokenOutputValue.TOKENS_ONLY,
                         builder.tokenOutput);
        assertExactValue("WikipediaTokenizerBuilder.untokenized_types", Sets.newHashSet(), builder.untokenizedTypes);
    }

    @Test(expected = IOException.class)
    public void testWikipediaTokenizerInvalidJSON() throws IOException {
        assertJsonParseFail("{type: \"wikipedia\", token_output: \"TOKENS_ONLY\", untoknized_types : [\"aaa\",\"bbb\"]}");
    }

    @Test(expected = IOException.class)
    public void testInvalidTokenizerType() throws IOException {
        assertJsonParseFail("{type: \"invalid_type\"}");
    }

}
