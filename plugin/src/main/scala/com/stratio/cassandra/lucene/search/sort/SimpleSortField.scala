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
package com.stratio.cassandra.lucene.search.sort

import java.util
import java.util.Collections

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.Schema
import com.stratio.cassandra.lucene.schema.mapping.Mapper
import org.apache.commons.lang3.StringUtils
import org.apache.lucene.search.SortField.FIELD_SCORE

/**
 * @author Eduardo Alonso `eduardoalonso@stratio.com`
  * @param field the name of field to sort by
 * @param reverse [[true]] if natural order should be reversed, {{{false} otherwise
 */
class SimpleSortField(val field : String,
                      val reverse : Boolean) extends SortField(reverse) {

    if (field == null || StringUtils.isBlank(field))  throw new IndexException("Field name required")
    /**
     * Returns the Lucene [[org.apache.lucene.search.SortField]] representing this [[SortField]].
     *
     * @param schema the [[Schema]] to be used
     * @return the equivalent Lucene sort field
     */
    override def sortField(schema: Schema) : org.apache.lucene.search.SortField = {
        if (field.equalsIgnoreCase("score")) {
            FIELD_SCORE
        } else {
            val mapper: Mapper = schema.mapper(field)
            if (mapper == null) {
                throw new IndexException("No mapper found for sortFields field '$field'")
            } else if (!mapper.docValues) {
                throw new IndexException("Field '$field' does not support sorting")
            } else {
                return mapper.sortField(field, reverse);
            }
        }
    }

    /** @inheritdoc */
    override def postProcessingFields() : util.Set[String] = Collections.singleton(field);

    /** @inheritdoc */
    override def toString: String =  MoreObjects.toStringHelper(this).add("field", field).add("reverse", reverse).toString()

    /** @inheritdoc */
    override def  equals(any: Any ) : Boolean = any match {
        case (ssF:SimpleSortField) => reverse == ssF.reverse && field.equals(ssF.field)
        case (other) => false
    }

    /** @inheritdoc */
    override def hashCode() :Int = {
        var result :Int = field.hashCode()
        result = 31 * result + (if (reverse) 1 else 0)
        result
    }
}