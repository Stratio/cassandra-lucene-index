package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer

/**
  * A {@link EdgeNGramTokenizer} for building {@link org.apache.lucene.analysis.ngram.EdgeNGramTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  * @param minGram the smallest n-gram to generate
  * @param maxGram the largest n-gram to generate
  */
case class EdgeNGramTokenizerBuilder(@JsonProperty("min_gram") final val minGram: Integer,
                                     @JsonProperty("max_gram") final val maxGram: Integer) extends TokenizerBuilder[EdgeNGramTokenizer] {
  /**
    * Builds a new {@link EdgeNGramTokenizer} using the specified minGram and manGram.
    *
    */
  override def function = () => {
    new EdgeNGramTokenizer(getOrDefault(Option(minGram), EdgeNGramTokenizerBuilder.DEFAULT_MIN_GRAM ).asInstanceOf[Integer],
                           getOrDefault(Option(maxGram), EdgeNGramTokenizerBuilder.DEFAULT_MAX_GRAM ).asInstanceOf[Integer])
  }

}

object EdgeNGramTokenizerBuilder {
  final val DEFAULT_MIN_GRAM = 1
  final val DEFAULT_MAX_GRAM = 1
}