/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.search.condition;

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import org.apache.commons.lang3.StringUtils;

/**
 * The abstract base class for queries directed to a specific field which name should be specified.
 *
 * Known subclasses are: <ul> <li> {@link FuzzyCondition} <li> {@link MatchCondition} <li> {@link PhraseCondition} <li>
 * {@link PrefixCondition} <li> {@link RangeCondition} <li> {@link WildcardCondition} <li> {@link BitemporalCondition}
 * <li> {@link DateRangeCondition} <li> {@link GeoDistanceCondition} <li> {@link GeoBBoxCondition} <li> {@link
 * GeoShapeCondition} </ul>
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleFieldCondition extends Condition {

    /** The name of the field to be matched. */
    public final String field;

    /**
     * Abstract {@link SingleFieldCondition} builder receiving the boost to be used.
     *
     * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     */
    public SingleFieldCondition(Float boost, String field) {
        super(boost);

        if (StringUtils.isBlank(field)) {
            throw new IndexException("Field name required");
        }

        this.field = field;
    }

    protected MoreObjects.ToStringHelper toStringHelper(Object o) {
        return super.toStringHelper(o).add("field", field);
    }
}
