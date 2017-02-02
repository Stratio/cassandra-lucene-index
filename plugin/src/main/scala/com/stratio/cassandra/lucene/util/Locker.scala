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

/** Class to execute code in mutual exclusion based on the hashcode of an object.
  *
  * @param numLocks the number of underlying concurrent locks
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class Locker(numLocks: Int) {

  if (numLocks <= 0) throw new IllegalArgumentException(
    s"The number of concurrent locks should be strictly positive but found $numLocks")

  private val locks = (1 to numLocks).map(_ => new Object()).toArray

  /** Runs the specified task in mutual exclusion based on the hashcode of the specified id */
  def run[A](id: AnyRef, task: () => A): Unit = {
    locks(Math.abs(id.hashCode % numLocks)).synchronized {task.apply()}
  }

}
