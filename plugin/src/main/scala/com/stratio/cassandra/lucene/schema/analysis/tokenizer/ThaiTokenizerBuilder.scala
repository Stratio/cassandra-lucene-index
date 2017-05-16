package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import org.apache.lucene.analysis.th.ThaiTokenizer

/**
  * A {@link ThaiTokenizer} for building {@link org.apache.lucene.analysis.th.ThaiTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  */
case class ThaiTokenizerBuilder() extends TokenizerBuilder[ThaiTokenizer] {
  /**
    * Builds a new {@link ThaiTokenizer}.
    */
  override def function = () => new ThaiTokenizer
}

object ThaiTokenizerBuilder {}