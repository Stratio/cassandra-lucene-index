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

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.lucene.analysis.Analyzer

/**
 * An Lucene [[Analyzer]] builder.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(Array(
                new JsonSubTypes.Type(value = classOf[ClasspathAnalyzerBuilder], name = "classpath"),
                new JsonSubTypes.Type(value = classOf[SnowballAnalyzerBuilder], name = "snowball")))
abstract class AnalyzerBuilder {

    /**
     * Gets or creates the Lucene [[Analyzer]].
     *
     * @return the built analyzer
     */
    def analyzer(): Analyzer
}
