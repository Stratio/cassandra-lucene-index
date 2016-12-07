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
package com.stratio.cassandra.lucene.util

import com.stratio.cassandra.lucene.BaseScalaTest

/** Class for testing [[TimeCounter]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class TimeCounterTest extends BaseScalaTest {

  test("reusable") {
    val started = TimeCounter.create.start
    Thread.sleep(10)
    assert(Range(10, 1000).contains(started.time))
    val stopped = started.stop
    assert(Range(10, 1000).contains(stopped.time))
    stopped.toString shouldBe s"${stopped.time} ms"

    Thread.sleep(1000)

    val newStarted = stopped.start
    Thread.sleep(10)
    assert(Range(20, 1000).contains(newStarted.time))
    val newStopped = newStarted.stop
    assert(Range(20, 1000).contains(newStopped.time))
    newStopped.toString shouldBe s"${newStopped.time} ms"
  }

  test("immutable") {
    val tc = TimeCounter.create
    tc.start.stop.start
    Thread.sleep(10)
    tc.time shouldBe 0
  }

}
