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

import org.slf4j.LoggerFactory
import org.apache.cassandra.tracing.Tracing

/** Wrapper for [[Tracing]] avoiding test environment failures.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
object Tracer {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Traces the specified string message.
    *
    * @param message the message to be traced
    */
  def trace(message: String) {
    try Tracing.trace(message) catch {
      case e: Error => logger.warn(s"Unable to trace: ${e.getMessage}", e)
    }
  }
}
