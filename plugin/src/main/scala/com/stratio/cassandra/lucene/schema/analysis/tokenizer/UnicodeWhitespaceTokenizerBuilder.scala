package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import org.apache.lucene.analysis.core.UnicodeWhitespaceTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.core.UnicodeWhitespaceTokenizer}
  *
  * @author Eduardo Alonso eduardoalonso@stratio.com
  */
case class UnicodeWhitespaceTokenizerBuilder() extends TokenizerBuilder[UnicodeWhitespaceTokenizer] {
  /**
    * Builds a new {@link UnicodeWhitespaceTokenizer}.
    */
    override def function = () => new UnicodeWhitespaceTokenizer
}

object UnicodeWhitespaceTokenizerBuilder {}