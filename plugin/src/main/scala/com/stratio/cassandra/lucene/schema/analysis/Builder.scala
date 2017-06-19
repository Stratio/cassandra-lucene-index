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
package com.stratio.cassandra.lucene.schema.analysis

import java.util
import collection.JavaConverters._
import scala.reflect.runtime.{universe=>ru}

/**
  * Implements the necessary functionality so that a 'case class' with attributes that extends from it,
  * can use reflection to construct a HashMap of attributes, necessary to build lucene factories
  *
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  */
trait Builder[T] {

  /**
    * Implement function to return Lucene factory. Mandatory override in instance class.
    *
    * @return the built factory
    */
  def buildFunction : () => T

  /**
    * Assistant function to throw Java Exceptions (Lucene's layer)
    *
    * @param throwable
    */
  def failThrowException(throwable: Throwable) = throw throwable

  /**
    * Wrap Java flow/exception (Lucene's layer) in Scala Style
    *
    * @return the built factory
    */
  def build: T = {
    import scala.util.control.Exception._
    catching(classOf[Exception]).either(buildFunction()).asInstanceOf[Either[Exception, T]].fold(failThrowException, x=>x)
  }

  /**
    * Set param if apply or default value in other case
    *
    * @param param        the main parameter.
    * @param defaultParam the default parameter if main paramaeter is null.
    * @return if (param!=null) { return param; }else{ return defaultParam; }
    */
  def getOrDefault(param: Option[Any], defaultParam: Any): Any = param.map(x => x).getOrElse(defaultParam)

  /**
    * Assistant function that return {@link TermSymbol} of an current instance
    *
    * @return iterable of Terms (reflection API)
    */
  def termSymbolsList = scala.reflect.runtime.currentMirror.classSymbol(this.getClass).toType
                          .members.collect { case m: ru.TermSymbol if m.isGetter => m }.map(_.asTerm)

  /**
    * Assistant function that return value of a {@link TermSymbol}
    *
    * @param termString TermSymbol to evaluate
    * @return value of TermSymbol
    */
  def reflectedFieldValue(termString: ru.TermSymbol) = ru.runtimeMirror(this.getClass.getClassLoader)
                                                        .reflect(this).reflectField(termString).get

  /**
    * Convert child instance parameters in Java {HashMap[String, String]}.
    * This function is usually called from the 'buildFunction' method overwritten in the child classes.
    *
    * @return Java {HashMap[String, String]} with key(parameterName)->value(parameterValue)
    */
  def mapParsed = new util.HashMap[String, String](termSymbolsList.collect({case tm: ru.TermSymbol if reflectedFieldValue(tm) != null => tm})
      .map(x => (x.name.toString, reflectedFieldValue(x).toString)).toMap.asJava)
}