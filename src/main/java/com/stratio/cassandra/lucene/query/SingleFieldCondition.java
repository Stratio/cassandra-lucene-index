/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.query;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperSingle;

/**
 * The abstract base class for queries.
 * <p/>
 * Known subclasses are: <ul> <li> {@link BooleanCondition} <li> {@link
 * FuzzyCondition} <li> {@link MatchCondition} <li>
 * {@link PhraseCondition} <li> {@link PrefixCondition}
 * <li> {@link RangeCondition} <li> {@link WildcardCondition}
 * </ul>
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class SingleFieldCondition extends Condition {

    /**
     * Abstract {@link SingleFieldCondition} builder receiving the boost to be used.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     *              weightings) have their score multiplied by {@code boost}.
     */
    public SingleFieldCondition(Float boost) {
        super(boost);
    }

    protected ColumnMapperSingle<?> getMapper(Schema schema, String field) {
        ColumnMapperSingle<?> columnMapper = schema.getMapperSingle(field);
        if (columnMapper == null) {
            throw new IllegalArgumentException("Not found mapper for field " + field);
        } else {
            return columnMapper;
        }
    }

}
