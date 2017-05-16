package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.lucene.analysis.core.KeywordTokenizer

/**
  * A {@link KeywordTokenizer} for building {@link org.apache.lucene.analysis.core.KeywordTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  * @param bufferSize the terms cache read buffer size
  */
case class KeywordTokenizerBuilder(@JsonProperty("buffer_size") final val bufferSize: Integer = KeywordTokenizerBuilder.DEFAULT_BUFFER_SIZE) extends TokenizerBuilder[KeywordTokenizer] {
  /**
    * Builds a new {@link KeywordTokenizerBuilder} using the specified buffer_size.
    *
    */
  override def function: () => KeywordTokenizer = () => new KeywordTokenizer(getOrDefault(Option(bufferSize), KeywordTokenizerBuilder.DEFAULT_BUFFER_SIZE ).asInstanceOf[Integer])
}


object KeywordTokenizerBuilder {
  final val DEFAULT_BUFFER_SIZE = 256
}

