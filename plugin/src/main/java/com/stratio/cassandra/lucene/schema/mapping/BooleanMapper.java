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

import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;

/**
 * A {@link Mapper} to map a boolean field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BooleanMapper extends KeywordMapper {

    /** The {@code String} representation of a true value. */
    private static final String TRUE = "true";

    /** The {@code String} representation of a false value. */
    private static final String FALSE = "false";

    /**
     * Builds a new {@link BooleanMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param indexed if the field supports searching
     * @param sorted if the field supports sorting
     * @param validated if the field must be validated
     */
    public BooleanMapper(String field, String column, Boolean indexed, Boolean sorted, Boolean validated) {
        super(field, column, indexed, sorted, validated, UTF8Type.instance, BooleanType.instance);
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value ? TRUE : FALSE;
        } else if (value instanceof String) {
            return base(name, (String) value);
        }
        throw new IndexException("Field '%s' requires a boolean, but found '%s'", name, value);
    }

    private String base(String name, String value) {
        if (value.equalsIgnoreCase(TRUE)) {
            return TRUE;
        } else if (value.equalsIgnoreCase(FALSE)) {
            return FALSE;
        } else {
            throw new IndexException("Field '%s' requires '%s' or '%s', but found '%s'", TRUE, FALSE, name, value);
        }
    }

}
