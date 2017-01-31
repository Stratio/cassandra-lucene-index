/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.index

import com.stratio.cassandra.lucene.BaseScalaTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[NoIDFSimilarity]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class NoIDFSimilarityTest extends BaseScalaTest {

  test("neutral IDF score") {
    val similarity = new NoIDFSimilarity
    similarity.idf(0l, 0l) shouldBe 1.0f
    similarity.idf(1l, 5l) shouldBe 1.0f
    similarity.idf(10000l, 10943l) shouldBe 1.0f
    similarity.idf(-45667l, 2132189l) shouldBe 1.0f
    similarity.idf(367423794l, -394612l) shouldBe 1.0f
    similarity.idf(-2147294213l, 15264214l) shouldBe 1.0f
  }
}
