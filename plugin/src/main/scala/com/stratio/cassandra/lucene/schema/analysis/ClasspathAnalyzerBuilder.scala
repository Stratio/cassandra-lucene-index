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

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.stratio.cassandra.lucene.IndexException
import org.apache.lucene.analysis.Analyzer

/**
 * [[AnalyzerBuilder]] for building [[Analyzer]]s in classpath using its default (no args) constructor.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @param className an [[Analyzer]] full qualified class name
 */
class ClasspathAnalyzerBuilder @JsonCreator() (@JsonProperty("class") className: String ) extends AnalyzerBuilder {

    /** @inheritdoc */
    override def analyzer(): Analyzer = {
        try {
            val analyzerClass = Class.forName(className)
            val constructor = analyzerClass.getConstructor()
            constructor.newInstance().asInstanceOf[Analyzer]
        } catch {
            case (e:Exception) =>throw new IndexException(e, "Not found analyzer '{}'", className);
        }
    }
}
