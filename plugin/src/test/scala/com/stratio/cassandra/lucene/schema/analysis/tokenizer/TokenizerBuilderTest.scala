package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.common.JsonSerializer
import org.apache.lucene.analysis.core._
import org.apache.lucene.analysis.ngram.{NGramTokenizer, EdgeNGramTokenizer}
import org.apache.lucene.analysis.path.{ReversePathHierarchyTokenizer, PathHierarchyTokenizer}
import org.apache.lucene.analysis.pattern.PatternTokenizer
import org.apache.lucene.analysis.standard.{UAX29URLEmailTokenizer, StandardTokenizer, ClassicTokenizer}
import org.apache.lucene.analysis.th.ThaiTokenizer
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.util.Try

/** Tests for [[TokenizerBuilder]].
  *
  * @author Juan Pedro Gilaberte `jpgilaberte@stratio.com`
  */

@RunWith(classOf[JUnitRunner])
class TokenizerBuilderTest extends BaseScalaTest{

  def failFlow(throwable: Throwable) = fail(throwable.getMessage, throwable)

  def buildAbstractBuilder(json: String, builderClass: Class[_]): Any = Try(JsonSerializer.fromString(json, builderClass)).fold(failFlow, x=>x)

  test("ClassicTokenizerBuilder parse JSON") {
    val abstractBuilder = buildAbstractBuilder("{type: \"classic\", max_token_length: 1}",
                                               classOf[TokenizerBuilder[ClassicTokenizer]]).asInstanceOf[TokenizerBuilder[ClassicTokenizer]]
    assert(classOf[ClassicTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    val tokenizer = abstractBuilder.buildTokenizer
    assert(classOf[ClassicTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
    assert(1 == tokenizer.getMaxTokenLength)
   }

  test("ClassicTokenizerBuilder parse JSON throw IllegalArgumentException") {
     val abstractBuilder = buildAbstractBuilder("{type: \"classic\", max_token_length: 0}",
     classOf[TokenizerBuilder[ClassicTokenizer]]).asInstanceOf[TokenizerBuilder[ClassicTokenizer]]
     assert(classOf[ClassicTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
     assertThrows[IllegalArgumentException]{abstractBuilder.buildTokenizer}
   }

  test("ClassicTokenizerBuilder parse JSON default values") {
     val abstractBuilder = buildAbstractBuilder("{type: \"classic\"}",
                                                classOf[TokenizerBuilder[ClassicTokenizer]]).asInstanceOf[TokenizerBuilder[ClassicTokenizer]]
     assert(classOf[ClassicTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
     val tokenizer = abstractBuilder.buildTokenizer
     assert(classOf[ClassicTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
     assert(ClassicTokenizerBuilder.DEFAULT_MAX_TOKEN_LENGTH == tokenizer.getMaxTokenLength)
   }

  test("EdgeNGramTokenizerBuilder parse JSON") {
     val abstractBuilder = buildAbstractBuilder("{type: \"edge_ngram\", min_gram: 1, max_gram: 2}",
     classOf[TokenizerBuilder[EdgeNGramTokenizer]]).asInstanceOf[TokenizerBuilder[EdgeNGramTokenizer]]
     assert(classOf[EdgeNGramTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
     val tokenizer = abstractBuilder.buildTokenizer
     assert(classOf[EdgeNGramTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
     assert(1 == abstractBuilder.asInstanceOf[EdgeNGramTokenizerBuilder].minGram)
     assert(2 == abstractBuilder.asInstanceOf[EdgeNGramTokenizerBuilder].maxGram)
   }

  test("EdgeNGramTokenizerBuilder parse JSON throws IllegalArgumentException") {
    val abstractBuilder = buildAbstractBuilder("{type: \"edge_ngram\", min_gram: -1, max_gram: 2}",
                                               classOf[TokenizerBuilder[EdgeNGramTokenizer]]).asInstanceOf[TokenizerBuilder[EdgeNGramTokenizer]]
    assert(classOf[EdgeNGramTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    assertThrows[IllegalArgumentException]{abstractBuilder.buildTokenizer}
  }

  test("KeywordTokenizerBuilder parse JSON") {
     val abstractBuilder = buildAbstractBuilder("{type: \"keyword\", buffer_size: 256}",
                                                classOf[TokenizerBuilder[KeywordTokenizer]]).asInstanceOf[TokenizerBuilder[KeywordTokenizer]]
     assert(classOf[KeywordTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
     val tokenizer = abstractBuilder.buildTokenizer
     assert(classOf[KeywordTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
     assert(256 == abstractBuilder.asInstanceOf[KeywordTokenizerBuilder].bufferSize)
   }

  test("KeywordTokenizerBuilder parse JSON throw IllegalArgumentException") {
    val abstractBuilder = buildAbstractBuilder("{type: \"keyword\", buffer_size: -256}",
                                               classOf[TokenizerBuilder[ClassicTokenizer]]).asInstanceOf[TokenizerBuilder[ClassicTokenizer]]
    assert(classOf[KeywordTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    assertThrows[IllegalArgumentException]{abstractBuilder.buildTokenizer}
  }

  test("LetterTokenizerBuilder parse JSON") {
     val abstractBuilder = buildAbstractBuilder("{type: \"letter\"}",
                                                classOf[TokenizerBuilder[LetterTokenizer]]).asInstanceOf[TokenizerBuilder[LetterTokenizer]]
     assert(classOf[LetterTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
     val tokenizer = abstractBuilder.buildTokenizer
     assert(classOf[LetterTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
   }

  test("LowerCaseTokenizerBuilder parse JSON") {
    val abstractBuilder = buildAbstractBuilder("{type: \"lower_case\"}",
                                               classOf[TokenizerBuilder[LowerCaseTokenizer]]).asInstanceOf[TokenizerBuilder[LowerCaseTokenizer]]
    assert(classOf[LowerCaseTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    val tokenizer = abstractBuilder.buildTokenizer
    assert(classOf[LowerCaseTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
  }

  test("NGramTokenizerBuilder parse JSON") {
     val abstractBuilder = buildAbstractBuilder("{type: \"ngram\", min_gram: 1, max_gram: 2}",
                                                classOf[TokenizerBuilder[NGramTokenizer]]).asInstanceOf[TokenizerBuilder[NGramTokenizer]]
     assert(classOf[NGramTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
     val tokenizer = abstractBuilder.buildTokenizer
     assert(classOf[NGramTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
     assert(1 == abstractBuilder.asInstanceOf[NGramTokenizerBuilder].minGram)
     assert(2 == abstractBuilder.asInstanceOf[NGramTokenizerBuilder].maxGram)
   }

  test("NGramTokenizerBuilder parse JSON throws IllegalArgumentException") {
     val abstractBuilder = buildAbstractBuilder("{type: \"ngram\", min_gram: -1, max_gram: 2}",
                                                classOf[TokenizerBuilder[NGramTokenizer]]).asInstanceOf[TokenizerBuilder[NGramTokenizer]]
     assert(classOf[NGramTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
     assertThrows[IllegalArgumentException]{abstractBuilder.buildTokenizer}
   }

  test("PathHierarchyTokenizerBuilder parse JSON") {
     val abstractBuilder = buildAbstractBuilder("{type: \"path_hierarchy\", buffer_size: 246, delimiter: \"$\", replacement: \"%\", skip: 3}",
                                                classOf[TokenizerBuilder[PathHierarchyTokenizer]]).asInstanceOf[TokenizerBuilder[PathHierarchyTokenizer]]
     assert(classOf[PathHierarchyTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
     val tokenizer = abstractBuilder.buildTokenizer
     assert(classOf[PathHierarchyTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
     assert(246 == abstractBuilder.asInstanceOf[PathHierarchyTokenizerBuilder].bufferSize)
     assert('$' == abstractBuilder.asInstanceOf[PathHierarchyTokenizerBuilder].delimiter)
     assert('%' == abstractBuilder.asInstanceOf[PathHierarchyTokenizerBuilder].replacement)
     assert(3 == abstractBuilder.asInstanceOf[PathHierarchyTokenizerBuilder].skip)
   }

  test("PathHierarchyTokenizerBuilder parse JSON throws IllegalArgumentException") {
     val abstractBuilder = buildAbstractBuilder("{type: \"path_hierarchy\", buffer_size: 246, delimiter: \"$\", replacement: \"%\", skip: -3}",
                                                classOf[TokenizerBuilder[PathHierarchyTokenizer]]).asInstanceOf[TokenizerBuilder[PathHierarchyTokenizer]]
     assert(classOf[PathHierarchyTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
     assertThrows[IllegalArgumentException]{abstractBuilder.buildTokenizer}
   }

  test("PatternTokenizerBuilder parse JSON") {
    val abstractBuilder = buildAbstractBuilder("{type: \"pattern\", pattern: \"[a-z]\", flags: 35, group: 0}",
      classOf[TokenizerBuilder[PatternTokenizer]]).asInstanceOf[TokenizerBuilder[PatternTokenizer]]
    assert(classOf[PatternTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    val tokenizer = abstractBuilder.buildTokenizer
    assert(classOf[PatternTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
    assert("[a-z]" == abstractBuilder.asInstanceOf[PatternTokenizerBuilder].pattern)
    assert(35 == abstractBuilder.asInstanceOf[PatternTokenizerBuilder].flags)
    assert(0 == abstractBuilder.asInstanceOf[PatternTokenizerBuilder].group)
  }

  test("PatternTokenizerBuilder parse JSON throws IllegalArgumentException") {
    val abstractBuilder = buildAbstractBuilder("{type: \"pattern\", pattern: \"[a-z]\", flags: 35, group: 2}",
      classOf[TokenizerBuilder[PatternTokenizer]]).asInstanceOf[TokenizerBuilder[PatternTokenizer]]
    assert(classOf[PatternTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    assertThrows[IllegalArgumentException]{abstractBuilder.buildTokenizer}
  }

  test("ReversePathHierarchyTokenizerBuilder parse JSON") {
    val abstractBuilder = buildAbstractBuilder("{type: \"reverse_path_hierarchy\", buffer_size: 246, delimiter: \"/\", replacement: \"%\", skip: 3}",
      classOf[TokenizerBuilder[ReversePathHierarchyTokenizer]]).asInstanceOf[TokenizerBuilder[ReversePathHierarchyTokenizer]]
    assert(classOf[ReversePathHierarchyTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    val tokenizer = abstractBuilder.buildTokenizer
    assert(classOf[ReversePathHierarchyTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
    assert(246 == abstractBuilder.asInstanceOf[ReversePathHierarchyTokenizerBuilder].bufferSize)
    assert('/' == abstractBuilder.asInstanceOf[ReversePathHierarchyTokenizerBuilder].delimiter)
    assert('%' == abstractBuilder.asInstanceOf[ReversePathHierarchyTokenizerBuilder].replacement)
    assert(3 == abstractBuilder.asInstanceOf[ReversePathHierarchyTokenizerBuilder].skip)
  }

  test("ReversePathHierarchyTokenizerBuilder parse JSON throws IllegalArgumentException") {
    val abstractBuilder = buildAbstractBuilder("{type: \"reverse_path_hierarchy\", buffer_size: 246, delimiter: \"/\", replacement: \"%\", skip: -3}",
      classOf[TokenizerBuilder[ReversePathHierarchyTokenizer]]).asInstanceOf[TokenizerBuilder[ReversePathHierarchyTokenizer]]
    assert(classOf[ReversePathHierarchyTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    assertThrows[IllegalArgumentException]{abstractBuilder.buildTokenizer}
  }

  test("StandardTokenizerBuilder parse JSON") {
    val abstractBuilder = buildAbstractBuilder("{type: \"standard\", max_token_length: 246}",
      classOf[TokenizerBuilder[StandardTokenizer]]).asInstanceOf[TokenizerBuilder[StandardTokenizer]]
    assert(classOf[StandardTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    val tokenizer = abstractBuilder.buildTokenizer
    assert(classOf[StandardTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
    assert(246 == abstractBuilder.asInstanceOf[StandardTokenizerBuilder].maxTokenLength)
  }

  test("StandardTokenizerBuilder parse JSON throws IllegalArgumentException") {
    val abstractBuilder = buildAbstractBuilder("{type: \"standard\", max_token_length: -246}",
      classOf[TokenizerBuilder[StandardTokenizer]]).asInstanceOf[TokenizerBuilder[StandardTokenizer]]
    assert(classOf[StandardTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    assertThrows[IllegalArgumentException]{abstractBuilder.buildTokenizer}
  }

  test("ThaiTokenizerBuilder parse JSON") {
    val abstractBuilder = buildAbstractBuilder("{type: \"thai\"}",
      classOf[TokenizerBuilder[ThaiTokenizer]]).asInstanceOf[TokenizerBuilder[ThaiTokenizer]]
    assert(classOf[ThaiTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    val tokenizer = abstractBuilder.buildTokenizer
    assert(classOf[ThaiTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
  }

  test("UAX29URLEmailTokenizerBuilder parse JSON") {
    val abstractBuilder = buildAbstractBuilder("{type: \"uax29_url_email\", max_token_length: 249}",
      classOf[TokenizerBuilder[UAX29URLEmailTokenizer]]).asInstanceOf[TokenizerBuilder[UAX29URLEmailTokenizer]]
    assert(classOf[UAX29URLEmailTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    val tokenizer = abstractBuilder.buildTokenizer
    assert(classOf[UAX29URLEmailTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
    assert(249 == abstractBuilder.asInstanceOf[UAX29URLEmailTokenizerBuilder].maxTokenLength)
  }

  test("UAX29URLEmailTokenizerBuilder parse JSON throws IllegalArgumentException") {
    val abstractBuilder = buildAbstractBuilder("{type: \"uax29_url_email\", max_token_length: -249}",
      classOf[TokenizerBuilder[UAX29URLEmailTokenizer]]).asInstanceOf[TokenizerBuilder[UAX29URLEmailTokenizer]]
    assert(classOf[UAX29URLEmailTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    assertThrows[IllegalArgumentException]{abstractBuilder.buildTokenizer}
  }

  test("UnicodeWhitespaceTokenizerBuilder parse JSON") {
    val abstractBuilder = buildAbstractBuilder("{type:\"unicode_whitespace\"}",
      classOf[TokenizerBuilder[UnicodeWhitespaceTokenizer]]).asInstanceOf[TokenizerBuilder[UnicodeWhitespaceTokenizer]]
    assert(classOf[UnicodeWhitespaceTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    val tokenizer = abstractBuilder.buildTokenizer
    assert(classOf[UnicodeWhitespaceTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
  }

  test("WhitespaceTokenizerBuilder parse JSON") {
    val abstractBuilder = buildAbstractBuilder("{type:\"whitespace\"}",
      classOf[TokenizerBuilder[WhitespaceTokenizer]]).asInstanceOf[TokenizerBuilder[WhitespaceTokenizer]]
    assert(classOf[WhitespaceTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    val tokenizer = abstractBuilder.buildTokenizer
    assert(classOf[WhitespaceTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
  }

  test("WikipediaTokenizerBuilder parse JSON") {
    val abstractBuilder = buildAbstractBuilder("{type: \"wikipedia\", token_output: \"TOKENS_ONLY\", untokenized_types : [\"aaa\",\"bbb\"]}",
      classOf[TokenizerBuilder[WikipediaTokenizer]]).asInstanceOf[TokenizerBuilder[WikipediaTokenizer]]
    assert(classOf[WikipediaTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    val tokenizer = abstractBuilder.buildTokenizer
    assert(classOf[WikipediaTokenizer].getCanonicalName == tokenizer.getClass.getCanonicalName)
    assert(TokenOutputEnum.TOKENS_ONLY.toString == abstractBuilder.asInstanceOf[WikipediaTokenizerBuilder].tokenOutput)
    assert(Array("aaa", "bbb").apply(1)  == abstractBuilder.asInstanceOf[WikipediaTokenizerBuilder].untokenizedTypes.apply(1))
  }

  test("WikipediaTokenizerBuilder parse JSON throws NoSuchElementException") {
    val abstractBuilder = buildAbstractBuilder("{type: \"wikipedia\", token_output: \"OKENS_ONLY\", untokenized_types : [\"aaa\",\"bbb\"]}",
      classOf[TokenizerBuilder[WikipediaTokenizer]]).asInstanceOf[TokenizerBuilder[WikipediaTokenizer]]
    assert(classOf[WikipediaTokenizerBuilder].getCanonicalName == abstractBuilder.getClass.getCanonicalName)
    assertThrows[NoSuchElementException]{abstractBuilder.buildTokenizer}
  }
}
