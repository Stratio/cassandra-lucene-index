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
package com.stratio.cassandra.lucene.search.sort.builder

import com.fasterxml.jackson.annotation.JsonProperty
import com.stratio.cassandra.lucene.search.sort.SimpleSortField

/**
 * @author Eduardo Alonso `eduardoalonso@stratio.com`
 * @param field The field to sort by.
 */
class SimpleSortFieldBuilder(@JsonProperty("field") val field : String) extends SortFieldBuilder[SimpleSortField, SimpleSortFieldBuilder] {
    /** @inheritdoc */
    override def build: SimpleSortField = new SimpleSortField(field, reverse)

    override var reverse: Boolean = _
}