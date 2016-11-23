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
package com.stratio.cassandra.lucene

import com.stratio.cassandra.lucene.IndexException._
import org.slf4j.helpers.MessageFormatter

/** [[RuntimeException]] to be thrown when there are Lucene index-related errors.
  *
  * @param message the detail message
  * @param cause   the cause
  * @author Andres de la Pena `adelapena@stratio.com`
  */
case class IndexException(
    message: String,
    cause: Throwable)
  extends RuntimeException(message, cause) {

  /** Constructs a new index exception with the specified cause.
    *
    * @param cause the cause
    */
  def this(cause: Throwable) = this(null, cause)


  /** Constructs a new index exception with the specified formatted detail message.
    *
    * @param message the detail message
    */
  def this(message: String) =
  this(message, null)

  /** Constructs a new index exception with the specified formatted detail message.
    *
    * @param message the detail message
    * @param a1      first argument
    */
  def this(message: String, a1: AnyRef) =
  this(format1(message, a1), null)

  /** Constructs a new index exception with the specified formatted detail message.
    *
    * @param message the detail message
    * @param a1      first argument
    * @param a2      second argument
    */
  def this(message: String, a1: AnyRef, a2: AnyRef) =
  this(format2(message, a1, a2), null)

  /** Constructs a new index exception with the specified formatted detail message.
    *
    * @param message the detail message
    * @param a1      first argument
    * @param a2      second argument
    * @param a3      third argument
    */
  def this(message: String, a1: AnyRef, a2: AnyRef, a3: AnyRef) =
  this(formatN(message, a1, a2, a3), null)

  /** Constructs a new index exception with the specified formatted detail message.
    *
    * @param message the detail message
    * @param a1      first argument
    * @param a2      second argument
    * @param a3      third argument
    * @param a4      fourth argument
    */
  def this(message: String, a1: AnyRef, a2: AnyRef, a3: AnyRef, a4: AnyRef) =
  this(formatN(message, a1, a2, a3, a4), null)

  /** Constructs a new index exception with the specified formatted detail message.
    *
    * @param cause   the cause
    * @param message the detail message
    */
  def this(cause: Throwable, message: String) =
  this(message, cause)

  /** Constructs a new index exception with the specified formatted detail message.
    *
    * @param cause   the cause
    * @param message the detail message
    * @param a1      first argument
    */
  def this(cause: Throwable, message: String, a1: AnyRef) =
  this(format1(message, a1), cause)

  /** Constructs a new index exception with the specified formatted detail message.
    *
    * @param cause   the cause
    * @param message the detail message
    * @param a1      first argument
    * @param a2      second argument
    */
  def this(cause: Throwable, message: String, a1: AnyRef, a2: AnyRef) =
  this(format2(message, a1, a2), cause)

  /** Constructs a new index exception with the specified formatted detail message.
    *
    * @param cause   the cause
    * @param message the detail message
    * @param a1      first argument
    * @param a2      second argument
    * @param a3      third argument
    */
  def this(cause: Throwable, message: String, a1: AnyRef, a2: AnyRef, a3: AnyRef) =
  this(formatN(message, a1, a2, a3), cause)

  /** Constructs a new index exception with the specified formatted detail message.
    *
    * @param cause   the cause
    * @param message the detail message
    * @param a1      first argument
    * @param a2      second argument
    * @param a3      third argument
    * @param a4      fourth argument
    */
  def this(cause: Throwable, message: String, a1: AnyRef, a2: AnyRef, a3: AnyRef, a4: AnyRef) =
  this(formatN(message, a1, a2, a3, a4), cause)

}

/** Companion object for [[IndexException]]. */
object IndexException {

  private def format1(message: String, arg: AnyRef): String = {
    MessageFormatter.format(message, arg).getMessage
  }

  private def format2(message: String, a1: AnyRef, a2: AnyRef): String = {
    MessageFormatter.format(message, a1, a2).getMessage
  }

  private def formatN(message: String, as: AnyRef*): String = {
    MessageFormatter.arrayFormat(message, as.toArray).getMessage
  }

}
