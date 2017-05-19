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
package com.stratio.cassandra.lucene.search.condition

import java.io.IOException

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.common.JsonSerializer
import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder
import org.junit.Assert.assertEquals

/**
  * Abstract class for [[ConditionBuilder]] tests.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
abstract class AbstractConditionTest extends BaseScalaTest {
  def testJsonSerialization(conditionBuilder: ConditionBuilder[_, _], json: String): Unit = {
    try {
      val json1 = JsonSerializer.toString(conditionBuilder)
      assertEquals("JSON serialization is wrong", json, json1)
      val json2 = JsonSerializer.toString(JsonSerializer.fromString(json1,
        classOf[ConditionBuilder]))
      assertEquals("JSON serialization is wrong", json1, json2)
    } catch {
      case (e: IOException) => throw new RuntimeException(e)
    }
  }
}
