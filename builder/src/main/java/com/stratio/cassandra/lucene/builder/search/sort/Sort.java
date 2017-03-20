/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
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
package com.stratio.cassandra.lucene.builder.search.sort;

import com.stratio.cassandra.lucene.builder.Builder;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Arrays;
import java.util.List;

/**
 * A sorting of fields for a search.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Sort extends Builder {

    /** The {@link SortField}s. */
    @JsonProperty("fields")
    final List<SortField> sortFields;

    /**
     * Creates a new {@link Sort} for the specified {@link SortField}.
     *
     * @param sortFields the {@link SortField}s
     */
    public Sort(List<SortField> sortFields) {
        this.sortFields = sortFields;
    }

    /**
     * Creates a new {@link Sort} for the specified {@link SortField}.
     *
     * @param sortFields the {@link SortField}s
     */
    @JsonCreator
    public Sort(@JsonProperty("fields") SortField... sortFields) {
        this(Arrays.asList(sortFields));
    }
}
