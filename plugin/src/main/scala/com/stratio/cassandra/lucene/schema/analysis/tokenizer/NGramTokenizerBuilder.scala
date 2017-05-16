package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.lucene.analysis.ngram.NGramTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.ngram.NGramTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  * @param minGram the smallest n-gram to generate
  * @param maxGram the largest n-gram to generate
  */
case class NGramTokenizerBuilder(@JsonProperty("min_gram") final val minGram: Integer, @JsonProperty("max_gram") final val maxGram: Integer) extends TokenizerBuilder[NGramTokenizer] {

  /**
    * Builds a new {@link NGramTokenizer} using the specified minGram and manGram.
    *
    */
    override def function = () => new NGramTokenizer(
    getOrDefault(Option(minGram), NGramTokenizerBuilder.DEFAULT_MIN_GRAM).asInstanceOf[Integer],
    getOrDefault(Option(maxGram), NGramTokenizerBuilder.DEFAULT_MAX_GRAM).asInstanceOf[Integer])

}

object NGramTokenizerBuilder {
  final val DEFAULT_MIN_GRAM = 1
  final val DEFAULT_MAX_GRAM = 2
}
