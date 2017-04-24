package com.stratio.cassandra.lucene.schema.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class CustomAnalyzer extends Analyzer {

    final Tokenizer tokenizer;



    public CustomAnalyzer(Tokenizer tokenizer) {
        this.tokenizer= tokenizer;

    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {

        TokenStream ts = tokenizer;

        return new TokenStreamComponents(tokenizer, ts);
    }


}
