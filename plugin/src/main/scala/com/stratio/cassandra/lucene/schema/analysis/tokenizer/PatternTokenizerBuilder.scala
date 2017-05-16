package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import java.util.regex.Pattern

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.lucene.analysis.pattern.PatternTokenizer

/**
  * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.pattern.PatternTokenizer}
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com>}
  * @param pattern if a token length is bigger that this, token is split at max token length intervals.
  * @param flags if a token length is bigger that this, token is split at max token length intervals.
  * @param group if a token length is bigger that this, token is split at max token length intervals.
  */
case class PatternTokenizerBuilder(@JsonProperty("pattern") final val pattern: String, @JsonProperty(
  "flags") final val flags: Integer, @JsonProperty("group") final val group: Integer) extends TokenizerBuilder[PatternTokenizer] {
  /**
    * Builds a new {@link KeywordTokenizerBuilder} using the specified maxTokenLength.
    *
    */
    override def function = () =>  new PatternTokenizer(Pattern.compile(
      getOrDefault(Option(pattern), PatternTokenizerBuilder.DEFAULT_PATTERN).asInstanceOf[String],
      getOrDefault(Option(flags), PatternTokenizerBuilder.DEFAULT_FLAGS).asInstanceOf[Integer]),
      getOrDefault(Option(group), PatternTokenizerBuilder.DEFAULT_GROUP).asInstanceOf[Integer])
}


object PatternTokenizerBuilder {
  final val DEFAULT_PATTERN = "\\W+"
  final val DEFAULT_FLAGS = 0
  final val DEFAULT_GROUP = -1
}