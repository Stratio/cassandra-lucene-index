package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.lucene.analysis.standard.StandardTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.standard.StandardTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  * @param maxTokenLength if a token length is bigger that this, token is split at max token length intervals.
  */
case class StandardTokenizerBuilder(@JsonProperty("max_token_length") final val maxTokenLength: Integer) extends TokenizerBuilder[StandardTokenizer] {
  /**
    * Builds a new {@link KeywordTokenizerBuilder} using the specified maxTokenLength.
    *
    */
    override def function = () => {
      val tokenizer: StandardTokenizer = new StandardTokenizer
      tokenizer.setMaxTokenLength(getOrDefault(Option(maxTokenLength), StandardTokenizerBuilder.DEFAULT_MAX_TOKEN_LENGTH).asInstanceOf[Int])
      tokenizer
    }
}

object StandardTokenizerBuilder {
  final val DEFAULT_MAX_TOKEN_LENGTH = 255
}