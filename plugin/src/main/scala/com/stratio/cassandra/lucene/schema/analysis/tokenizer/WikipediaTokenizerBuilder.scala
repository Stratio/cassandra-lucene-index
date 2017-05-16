package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.Sets
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer

import scala.collection.immutable.ListSet

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.wikipedia.WikipediaTokenizer}
  *
  * @author Eduardo Alonso eduardoalonso@stratio.com
  * @param tokenOutput this tokenizer output, only untokenized, only tokens or both
  * @param untokenizedTypes //TODO
  */
case class WikipediaTokenizerBuilder(@JsonProperty("token_output") tokenOutput: String,
                                     @JsonProperty("untokenized_types") final val untokenizedTypes: Array[String]) extends TokenizerBuilder[WikipediaTokenizer]{
  /**
    * Builds a new {@link WikipediaTokenizerBuilder} using the specified tokenOutput and untokenizedTypes.
    *
    */
  override def function = () => {
    import collection.JavaConverters._
    new WikipediaTokenizer(getOrDefault(Option(TokenOutputEnum.withName(tokenOutput).id), TokenOutputEnum.TOKENS_ONLY).asInstanceOf[Int], untokenizedTypes.toSet.asJava)
  }
}

object WikipediaTokenizer {
  final val DEFAULT_TOKEN_OUTPUT = TokenOutputEnum.TOKENS_ONLY
}

object TokenOutputEnum extends Enumeration{
  type TokenOutputValue = Value
  val TOKENS_ONLY = Value("TOKENS_ONLY")
  val UNTOKENIZED_ONLY = Value("UNTOKENIZED_ONLY")
  val BOTH = Value("BOTH")
}
