package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import org.apache.lucene.analysis.core.LetterTokenizer

/**
  * A {@link LetterTokenizer} for building {@link org.apache.lucene.analysis.core.LetterTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  */
case class  LetterTokenizerBuilder() extends TokenizerBuilder[LetterTokenizer] {
  /**
    * Builds a new {@link LetterTokenizer}.
    */
  override def function: () => LetterTokenizer = () => new LetterTokenizer
}

object LetterTokenizerBuilder {}


