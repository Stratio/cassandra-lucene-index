/**
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
package com.stratio.cassandra.lucene.search.sort.builder;

import com.stratio.cassandra.lucene.search.sort.Sort;
import com.stratio.cassandra.lucene.search.sort.SortField;
import com.stratio.cassandra.lucene.util.Builder;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link Builder} for building a new {@link Sort}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SortBuilder implements Builder<Sort> {

    /** The {@link SortField}s. */
    @JsonProperty("fields")
    final List<SortFieldBuilder> sortFieldBuilders;

    /**
     * Creates a new {@link SortBuilder} for the specified {@link SortFieldBuilder}.
     *
     * @param sortFieldBuilders The {@link SortFieldBuilder}s.
     */
    public SortBuilder(List<SortFieldBuilder> sortFieldBuilders) {
        this.sortFieldBuilders = sortFieldBuilders;
    }

    /**
     * Creates a new {@link SortBuilder} for the specified {@link SortFieldBuilder}.
     *
     * @param sortFieldBuilders The {@link SortFieldBuilder}s.
     */
    @JsonCreator
    public SortBuilder(@JsonProperty("fields") SortFieldBuilder... sortFieldBuilders) {
        this(Arrays.asList(sortFieldBuilders));
    }

    /** {@inheritDoc} */
    @Override
    public Sort build() {
        return new Sort(sortFieldBuilders.stream().map(SortFieldBuilder::build).collect(Collectors.toList()));
    }
}
