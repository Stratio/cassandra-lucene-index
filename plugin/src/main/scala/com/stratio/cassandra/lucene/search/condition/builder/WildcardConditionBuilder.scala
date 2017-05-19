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
package com.stratio.cassandra.lucene.search.condition.builder

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.stratio.cassandra.lucene.search.condition.WildcardCondition

/**
 * [[ConditionBuilder]] for building a new [[WildcardCondition]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param field the name of the field to be matched
 * @param value the wildcard expression to be matched
 */
class WildcardConditionBuilder @JsonCreator() ( @JsonProperty("field") val field: String,
                                                @JsonProperty("value") val value: String) extends ConditionBuilder[WildcardCondition, WildcardConditionBuilder] {

    /** @inheritdoc*/
    override def build: WildcardCondition =
            new WildcardCondition(boost, field, value)
}
