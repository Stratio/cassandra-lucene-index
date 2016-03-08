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

package com.stratio.cassandra.lucene.schema.mapping;

import org.apache.cassandra.db.marshal.*;

/**
 * A {@link Mapper} to map a string, not tokenized field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class StringMapper extends KeywordMapper {

    /** The default case sensitive option. */
    public static final boolean DEFAULT_CASE_SENSITIVE = true;

    /** If it must be case sensitive. */
    public final boolean caseSensitive;

    /**
     * Builds a new {@link StringMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param indexed if the field supports searching
     * @param sorted if the field supports sorting
     * @param validated if the field must be validated
     * @param caseSensitive if the analyzer must be case sensitive
     */
    public StringMapper(String field,
                        String column,
                        Boolean indexed,
                        Boolean sorted,
                        Boolean validated,
                        Boolean caseSensitive) {
        super(field,
              column,
              indexed,
              sorted,
              validated,
              AsciiType.instance,
              BooleanType.instance,
              BytesType.instance,
              ByteType.instance,
              DoubleType.instance,
              FloatType.instance,
              InetAddressType.instance,
              IntegerType.instance,
              Int32Type.instance,
              LongType.instance,
              ShortType.instance,
              TimestampType.instance,
              TimeUUIDType.instance,
              UTF8Type.instance,
              UUIDType.instance);
        this.caseSensitive = caseSensitive == null ? DEFAULT_CASE_SENSITIVE : caseSensitive;
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {
        String string = value.toString();
        return caseSensitive ? string : string.toLowerCase();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("caseSensitive", caseSensitive).toString();
    }
}
