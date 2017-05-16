package com.stratio.cassandra.lucene.schema.analysis.tokenizer


import org.apache.lucene.analysis.core.LowerCaseTokenizer

/**
  * A {@link LowerCaseTokenizer} for building {@link org.apache.lucene.analysis.core.LowerCaseTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  */
case class LowerCaseTokenizerBuilder() extends TokenizerBuilder[LowerCaseTokenizer]{
  /**
    * Builds a new {@link LowerCaseTokenizer}.
    */
  override def function: () => LowerCaseTokenizer = () => new LowerCaseTokenizer
}

object LowerCaseTokenizerBuilder {}