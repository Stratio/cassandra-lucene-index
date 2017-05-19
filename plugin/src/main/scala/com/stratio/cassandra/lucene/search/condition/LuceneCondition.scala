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
package com.stratio.cassandra.lucene.search.condition

import java.util
import java.util.{LinkedHashSet, Set}

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.Schema
import org.apache.commons.lang3.StringUtils
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.queryparser.classic.{ParseException, QueryParser}
import org.apache.lucene.search.Query

/**
 * A [[Condition]] implementation that matches documents satisfying a Lucene Query Syntax.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param query the Lucene Query Syntax query
 * @param defaultField the default field name
 */
class LuceneCondition(  val boost : java.lang.Float,
                        val query: String,
                        var defaultField: String) extends Condition(boost) {

    if (StringUtils.isBlank(query)) {
        throw new IndexException("Query statement required");
    }
    defaultField = if (defaultField == null) LuceneCondition.DEFAULT_FIELD else defaultField

    /** @inheritdoc */
    def postProcessingFields() : util.Set[String] = {
        val fields : util.Set[String] = new util.LinkedHashSet()
        if (!StringUtils.isBlank(defaultField)) {
            fields.add(defaultField)
        }
        for (term <- query.split("[ ]")) {
            if (term.contains(":")) {
                fields.add(term.split(":")(0))
            }
        }
        fields
    }

    /** @inheritdoc */
    override def doQuery(schema : Schema) : Query = {
        try {
            val analyzer : Analyzer  = schema.analyzer
            val queryParser : QueryParser  = new QueryParser(defaultField, analyzer)
            queryParser.setAllowLeadingWildcard(true)
            queryParser.setLowercaseExpandedTerms(false)
            queryParser.parse(query)
        } catch {
            case (e: ParseException) => throw new IndexException("Error while parsing lucene syntax query: {}", e.getMessage());
        }
    }

    /** @inheritdoc */
    override def toStringHelper : MoreObjects.ToStringHelper =
        toStringHelper(this).add("query", query).add("defaultField", defaultField)

}

object LuceneCondition {
    /** The default name of the field where the clauses will be applied by default. */
    def DEFAULT_FIELD : String = "lucene"
}