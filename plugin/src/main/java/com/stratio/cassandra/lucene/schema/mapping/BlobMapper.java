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
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.Hex;

import java.nio.ByteBuffer;

/**
 * A {@link Mapper} to map blob values.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BlobMapper extends KeywordMapper {

    /**
     * Builds a new {@link BlobMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param indexed if the field supports searching
     * @param sorted if the field supports sorting
     * @param validated if the field must be validated
     */
    public BlobMapper(String field, String column, Boolean indexed, Boolean sorted, Boolean validated) {
        super(field, column, indexed, sorted, validated, UTF8Type.instance, BytesType.instance);
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {
        if (value instanceof ByteBuffer) {
            return base((ByteBuffer) value);
        } else if (value instanceof byte[]) {
            return base((byte[]) value);
        } else if (value instanceof String) {
            return base((String) value);
        }
        throw new IndexException("Field '%s' requires a byte array, but found '%s'", field, value);
    }

    private String base(ByteBuffer value) {
        return ByteBufferUtils.toHex(value);
    }

    private String base(byte[] value) {
        return ByteBufferUtils.toHex(value);
    }

    private String base(String value) {
        try {
            byte[] bytes = Hex.hexToBytes(value.replaceFirst("0x", ""));
            return Hex.bytesToHex(bytes);
        } catch (NumberFormatException e) {
            throw new IndexException(e, "Field '%s' requires an hex string, but found '%s'", field, value);
        }
    }
}
