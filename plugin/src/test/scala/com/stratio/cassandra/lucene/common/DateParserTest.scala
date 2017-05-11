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
package com.stratio.cassandra.lucene.common

import java.text.DateFormat
import java.util.Date

import com.stratio.cassandra.lucene.BaseScalaTest
import org.apache.cassandra.utils.UUIDGen
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
/**
  * @author Eduardo Alonso  `eduardoalonso@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class DateParserTest extends BaseScalaTest {
  test("parse null") {
    BaseScalaTest.assertNull("yyyy/MM/dd", null)
  }

  test("parse date ") {
    val date = BaseScalaTest.date("yyyy/MM/dd", "2015/11/03")
    BaseScalaTest.assertEquals("yyyy/MM/dd", date, date)
  }

  test("parse min date") {
    val date = new Date(0)
    BaseScalaTest.assertEquals(DateParser.DEFAULT_PATTERN, date, date)
  }

  test("parse max date") {
    val date = new Date(Long.MaxValue)
    BaseScalaTest.assertEquals(DateParser.DEFAULT_PATTERN, date, date)
  }


  test("parse date truncating") {
    val date = BaseScalaTest.date("yyyy/MM/dd HH:mm:ss", "2015/11/03 01:02:03")
    val  expected = BaseScalaTest.date("yyyy/MM/dd", "2015/11/03")
    BaseScalaTest.assertEquals("yyyy/MM/dd", date, expected)
  }

  test("parse integer") {
    val date = BaseScalaTest.date("yyyyMMdd", "20151103")
    BaseScalaTest.assertEquals("yyyyMMdd", 20151103, date)
  }

  test("parse invalid integer") {
    BaseScalaTest.assertFail("yyyyMMdd", 1)
  }

  test("parse negative integer") {
    BaseScalaTest.assertFail("yyyyMMdd", -20151103)
  }

  test("parse long") {
    val date = BaseScalaTest.date("yyyyMMdd", "20151103")
    BaseScalaTest.assertEquals("yyyyMMdd", 20151103L, date)
  }

  test("parse invalid long") {
    BaseScalaTest.assertFail("yyyyMMdd", 1L)
  }

  test("parse negative long ") {
    BaseScalaTest.assertFail("yyyyMMdd", -20151103L)
  }

  test("parse float") {
    val date = BaseScalaTest.date("yyyy", "2015")
    BaseScalaTest.assertEquals("yyyy", 2015f, date)
  }

  test("parse decimal float") {
    val date = BaseScalaTest.date("yyyy", "2015")
    BaseScalaTest.assertEquals("yyyy", 2015.7f, date)
  }

  test("parse invalid float") {
    BaseScalaTest.assertFail("yyMM", 1f)
  }

  test("parse negative float") {
    BaseScalaTest.assertFail("yyyy", -2015f)
  }

  test("parse double") {
    val date = BaseScalaTest.date("yyyyMM", "201511")
    BaseScalaTest.assertEquals("yyyyMM", 201511d, date)
  }

  test("parse decimal double") {
    val date = BaseScalaTest.date("yyyyMM", "201511")
    BaseScalaTest.assertEquals("yyyyMM", 201511.7d, date)
  }

  test("parse invalid double") {
    BaseScalaTest.assertFail("yyyyMM", 1d)
  }

  test("parse negative double") {
    BaseScalaTest.assertFail("yyyyMM", -201511d)
  }

  test("parse string") {
    val expected = BaseScalaTest.date("yyyy/MM/dd", "2015/11/03")
    BaseScalaTest.assertEquals("yyyy/MM/dd", "2015/11/03", expected)
  }
  test("parse invalid string") {
    BaseScalaTest.assertFail("yyyy/MM/dd", "20151103")
  }

  test("parse uuid") {
    val uuid = UUIDGen.getTimeUUID(BaseScalaTest.date("yyyy-MM-dd HH:mm", "2015-11-03 06:23").getTime)
    val expected = BaseScalaTest.date("yyyyMMdd", "20151103")
    BaseScalaTest.assertEquals("yyyyMMdd", uuid, expected)
  }
}
