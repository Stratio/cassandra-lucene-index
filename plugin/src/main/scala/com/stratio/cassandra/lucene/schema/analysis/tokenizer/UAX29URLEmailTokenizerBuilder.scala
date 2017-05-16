package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer}
  *
  * @author Eduardo Alonso eduardoalonso@stratio.com
  * @param maxTokenLength if a token length is bigger that this, token is split at max token length intervals.
  */
case class UAX29URLEmailTokenizerBuilder(@JsonProperty("max_token_length") final val maxTokenLength: Integer) extends TokenizerBuilder[UAX29URLEmailTokenizer] {
  /**
    * Builds a new {@link UAX29URLEmailTokenizer} using the specified maxTokenLength.
    *
    */
    override def function = () => {
      val tokenizer: UAX29URLEmailTokenizer = new UAX29URLEmailTokenizer
      tokenizer.setMaxTokenLength(getOrDefault(Option(maxTokenLength), UAX29URLEmailTokenizerBuilder.DEFAULT_MAX_TOKEN_LENGHT).asInstanceOf[Int])
      tokenizer
    }
}

object UAX29URLEmailTokenizerBuilder {
  final val DEFAULT_MAX_TOKEN_LENGHT = 255
}