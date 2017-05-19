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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.IOException

import scala.reflect.ClassTag
import scala.reflect._

/**
 * A JSON mapper based on Codehaus [[ObjectMapper]] annotations.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
object JsonSerializer {

    def mapper : ObjectMapper = new ObjectMapper()
      .configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, false)
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
      .configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
      .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
      .setSerializationInclusion(JsonInclude.Include.NON_NULL)

    /**
      * Returns the JSON [[String]] representation of the specified object.
      *
      * @param value the object to be serialized.
      * @return the JSON [[String]] representation of value}
      * @throws IOException if there are serialization problems
      */
    @throws(classOf[IOException])
    def toString(value : Any) : String  =
        mapper.writeValueAsString(value)

    /**
     * Returns the object of the specified class represented by the specified JSON [[String]].
     *
     * @param value the JSON [[String]] to be parsed
     * @param valueType the class of the object to be parsed
     * @tparam T the type of the object to be parsed
     * @return an object of the specified class represented by {{{value}}}
     * @throws IOException if there are parsing problems
     */
    @throws(classOf[IOException])
    def fromString[T](value: String , valueType : Class[T]): T =
        mapper.readValue(value, valueType)

}


