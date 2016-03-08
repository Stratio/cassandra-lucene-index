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

package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.BlobMapper;

/**
 * {@link SingleColumnMapperBuilder} to build a new {@link BlobMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BlobMapperBuilder extends SingleColumnMapperBuilder<BlobMapper, BlobMapperBuilder> {

    /**
     * Returns the {@link BlobMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link BlobMapper} represented by this
     */
    @Override
    public BlobMapper build(String field) {
        return new BlobMapper(field, column, indexed, sorted, validated);
    }
}
