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

import java.util.Optional
import java.util.concurrent.Callable
import java.util.function.BiFunction
import java.util.function.Function

/** Implicit Scala to Java conversions.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
object JavaConversions {

  implicit def asJavaCallable[A](f:() => A): Callable[A] = {
    new Callable[A] {override def call: A = f.apply}
  }

  implicit def asJavaRunnable(f:() => Unit): Runnable = {
    new Runnable {override def run(): Unit = f.apply}
  }

  implicit def asJavaFunction[A, B](f: A => B): Function[A, B] = {
    new Function[A,B] {override def apply(a: A): B = f(a)}
  }

  implicit def asJavaBiFunction[A, B, C](sf: (A, B) => C): BiFunction[A, B, C] = {
    new BiFunction[A,B,C] {override def apply(a: A, b: B): C = sf(a, b)}
  }

  implicit def asJavaOptional[A](o:Option[A]):Optional[A] = o match {
    case Some(a) => Optional.of(a)
    case None => Optional.empty()
  }
}
