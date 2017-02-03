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

import java.util.concurrent.{Executors, TimeUnit}

import com.stratio.cassandra.lucene.BaseScalaTest

/** Class for testing [[Locker]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class LockerTest extends BaseScalaTest {

  test("test locker synchronization") {

    val executor = Executors.newFixedThreadPool(8)
    val locker = new Locker(4)

    val numIncrements = 10000
    val numCounters = 8
    val counters = (1 to numCounters).map(_ => 0).toArray

    (0 until numCounters).foreach { counter =>
      (0 until numIncrements).foreach { _ =>
        executor.submit[Unit](() =>
          locker.run(counter.asInstanceOf[AnyRef], () => {counters(counter) += 1}))
      }
    }

    (0 until numIncrements).foreach { _ =>
      (0 until numCounters).foreach { counter =>
        executor.submit[Unit](() =>
          locker.run(counter.asInstanceOf[AnyRef], () => {counters(counter) -= 1}))
      }
    }

    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)

    counters shouldBe (1 to numCounters).map(_ => 0).toArray
  }
}
