package com.stratio.cassandra.lucene.builder.index.schema.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenizer.Tokenizer;

/**
 * {@link Analyzer} using a Lucene's {@code Analyzer}s in classpath.
 *
 * It's uses the {@code Analyzer}'s default (no args) constructor.
 *
 * @author Juan Pedro Gilaberte {@literal <jpgilaberte@stratio.com>}
 */
public class CustomAnalyzer extends Analyzer{

    @JsonProperty("tokenizer")
    private final Tokenizer tokenizer;

    /**
     * Builds a new {@link CustomAnalyzer} using custom tokenizer, char_filters and token_filters.
     *
     * @param tokenizer an {@link Tokenizer} the tookenizer to use.
     */
    @JsonCreator
    public CustomAnalyzer(@JsonProperty("tokenizer") Tokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }
}
