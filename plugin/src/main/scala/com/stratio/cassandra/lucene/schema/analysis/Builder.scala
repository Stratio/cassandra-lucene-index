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
  * @author Juan Pedro Gilaberte jpgilaberte@stratio.com
  */
trait Builder[T] {

  /**
    * Implement function to return Lucene object
    *
    * @return
    */
  def buildFunction : () => T

  //TODO: refactor scala style (remove throw and manage exception in centralized layer)
  /**
    * Auxiliary function to manage Java Exceptions (Lucene's layer)
    *
    * @param throwable
    * @return
    */
  def failThrowException(throwable: Throwable) = throw throwable

  /**
    * Manage Java Exceptions (Lucene's layer)
    *
    * @return the built analyzer
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
    *
    * @return
    */
  def termSymbolsList = scala.reflect.runtime.currentMirror.classSymbol(this.getClass).toType
                          .members.collect { case m: ru.TermSymbol if m.isGetter => m }.map(_.asTerm)

  /**
    *
    * @param termString
    * @return
    */
  def reflectedFieldValue(termString: ru.TermSymbol) = ru.runtimeMirror(this.getClass.getClassLoader)
                                                        .reflect(this).reflectField(termString).get

  /**
    *
     * @return
    */
  def mapParsed = new util.HashMap[String, String](termSymbolsList.collect({case tm: ru.TermSymbol if reflectedFieldValue(tm) != null => tm})
      .map(x => (x.name.toString, reflectedFieldValue(x).toString)).toMap.asJava)
}