package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import org.apache.lucene.analysis.core.WhitespaceTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.core.WhitespaceTokenizer}
  *
  * @author Eduardo Alonso duardoalonso@stratio.com
  */
case class WhitespaceTokenizerBuilder() extends TokenizerBuilder[WhitespaceTokenizer]{
  /**
    * Builds a new {@link WhitespaceTokenizer}.
    */
    override def function = () => new WhitespaceTokenizer
}

object WhitespaceTokenizerBuilder {}