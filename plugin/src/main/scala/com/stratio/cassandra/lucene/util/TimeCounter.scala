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

/** Immutable class for measuring time durations in milliseconds.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
sealed abstract class TimeCounter {

  /** Returns the measured time in milliseconds.
    *
    * @return the measured time in milliseconds
    */
  def time: Long

  /** @inheritdoc */
  override def toString: String = s"$time ms"

}

/** A started [[TimeCounter]].
  *
  * @param startTime the start time in milliseconds
  * @param runTime   the already run time in milliseconds
  */
class StartedTimeCounter(startTime: Long, runTime: Long) extends TimeCounter {

  /** @inheritdoc */
  override def time: Long = runTime + System.currentTimeMillis - startTime

  /** Returns a new stopped time counter.
    *
    * @return a new stopped time counter
    */
  def stop: StoppedTimeCounter = new StoppedTimeCounter(time)

  /** @inheritdoc */
  override def toString: String = s"$time ms"

}

/** A stopped [[TimeCounter]].
  *
  * @param runTime the total run time in milliseconds
  */
class StoppedTimeCounter(runTime: Long) extends TimeCounter {

  /** @inheritdoc */
  override def time: Long = runTime

  /** Returns a new started time counter.
    *
    * @return a new started time counter
    */
  def start: StartedTimeCounter = new StartedTimeCounter(System.currentTimeMillis, time)

}

/** Companion object for [[TimeCounter]]. */
object TimeCounter {

  /** Returns a new [[StoppedTimeCounter]].
    *
    * @return a new stopped time counter
    */
  def create: StoppedTimeCounter = new StoppedTimeCounter(0)

  /** Returns a new [[StartedTimeCounter]].
    *
    * @return a new started time counter
    */
  def start: StartedTimeCounter = create.start

  /** Runs the specified closure and returns a stopped time counter measuring its execution time.
    *
    * @param f the closure to be run and measured
    * @return a new stopped time counter
    */
  def apply(f: () => Unit): StoppedTimeCounter = {
    val counter = create.start
    f.apply()
    counter.stop
  }

}
