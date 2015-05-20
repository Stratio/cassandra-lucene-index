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
package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.Hex;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;

/**
 * A {@link ColumnMapper} to map blob values.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperBlob extends ColumnMapperKeyword {

    /**
     * Builds a new {@link ColumnMapperBlob}.
     *
     * @param indexed If the field supports searching.
     * @param sorted  If the field supports sorting.
     */
    @JsonCreator
    public ColumnMapperBlob(@JsonProperty("indexed") Boolean indexed, @JsonProperty("sorted") Boolean sorted) {
        super(indexed, sorted, AsciiType.instance, UTF8Type.instance, BytesType.instance);
    }

    /** {@inheritDoc} */
    @Override
    public String base(String name, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof ByteBuffer) {
            ByteBuffer bb = (ByteBuffer) value;
            return ByteBufferUtils.toHex(bb);
        } else if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            return ByteBufferUtils.toHex(bytes);
        } else if (value instanceof String) {
            String string = (String) value;
            string = string.replaceFirst("0x", "");
            byte[] bytes = Hex.hexToBytes(string);
            return Hex.bytesToHex(bytes);
        } else {
            return error("Field '%s' requires a byte array, but found '%s'", name, value);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("indexed", indexed).add("sorted", sorted).toString();
    }
}
