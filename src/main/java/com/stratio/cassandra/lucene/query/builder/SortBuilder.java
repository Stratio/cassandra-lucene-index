/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.Sort;
import com.stratio.cassandra.lucene.query.SortField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@link Builder} for building a new {@link Sort}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SortBuilder implements Builder<Sort> {

    /** The {@link SortField}s */
    private final List<SortField> sortFields;

    /**
     * Creates a new {@link SortBuilder} for the specified {@link SortFieldBuilder}.
     *
     * @param sortFieldBuilders The {@link SortFieldBuilder}s.
     */
    public SortBuilder(List<SortFieldBuilder> sortFieldBuilders) {
        this.sortFields = new ArrayList<>(sortFieldBuilders.size());
        for (SortFieldBuilder sortFieldBuilder : sortFieldBuilders) {
            sortFields.add(sortFieldBuilder.build());
        }
    }

    /**
     * Creates a new {@link SortBuilder} for the specified {@link SortFieldBuilder}.
     *
     * @param sortFieldBuilders The {@link SortFieldBuilder}s.
     */
    public SortBuilder(SortFieldBuilder... sortFieldBuilders) {
        this(Arrays.asList(sortFieldBuilders));
    }

    /** {@inheritDoc} */
    @Override
    public Sort build() {
        return new Sort(sortFields);
    }
}
