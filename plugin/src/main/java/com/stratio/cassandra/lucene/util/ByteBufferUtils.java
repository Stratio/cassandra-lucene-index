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

package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Utility class with some {@link ByteBuffer}/{@link AbstractType} utilities.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class ByteBufferUtils {

    /** Private constructor to hide the implicit public one. */
    private ByteBufferUtils() {
    }

    /**
     * Returns the specified {@link ByteBuffer} as a byte array.
     *
     * @param byteBuffer a {@link ByteBuffer} to be converted to a byte array.
     * @return the byte array representation of the {@code byteBuffer}.
     */
    public static byte[] asArray(ByteBuffer byteBuffer) {
        ByteBuffer bb = ByteBufferUtil.clone(byteBuffer);
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return bytes;
    }

    /**
     * Returns {@code true} if the specified {@link ByteBuffer} is empty, {@code false} otherwise.
     *
     * @param byteBuffer A {@link ByteBuffer}.
     * @return {@code true} if the specified {@link ByteBuffer} is empty, {@code false} otherwise.
     */
    public static boolean isEmpty(ByteBuffer byteBuffer) {
        return byteBuffer.remaining() == 0;
    }

    /**
     * Returns the {@link ByteBuffer}s contained in {@code byteBuffer} according to {@code type}.
     *
     * @param byteBuffer the {@link ByteBuffer} to be split.
     * @param type       the {@link AbstractType} of {@code byteBuffer}.
     * @return the {@link ByteBuffer}s contained in {@code byteBuffer} according to {@code type}.
     */
    public static ByteBuffer[] split(ByteBuffer byteBuffer, AbstractType<?> type) {
        if (type instanceof CompositeType) {
            return ((CompositeType) type).split(byteBuffer);
        } else {
            return new ByteBuffer[]{byteBuffer};
        }
    }

    /**
     * Returns a {@code String} representation of {@code byteBuffer} validated by {@code type}.
     *
     * @param byteBuffer the {@link ByteBuffer} to be converted to {@code String}.
     * @param type       {@link AbstractType} of {@code byteBuffer}.
     * @return a {@code String} representation of {@code byteBuffer} validated by {@code type}.
     */
    public static String toString(ByteBuffer byteBuffer, AbstractType<?> type) {
        if (type instanceof CompositeType) {
            CompositeType composite = (CompositeType) type;
            List<AbstractType<?>> types = composite.types;
            ByteBuffer[] components = composite.split(byteBuffer);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < components.length; i++) {
                AbstractType<?> componentType = types.get(i);
                ByteBuffer component = components[i];
                sb.append(componentType.compose(component));
                if (i < types.size() - 1) {
                    sb.append(':');
                }
            }
            return sb.toString();
        } else {
            return type.compose(byteBuffer).toString();
        }
    }

    /**
     * Returns the hexadecimal {@code String} representation of the specified {@link ByteBuffer}.
     *
     * @param byteBuffer A {@link ByteBuffer}.
     * @return The hexadecimal {@code String} representation of the specified {@link ByteBuffer}.
     */
    public static String toHex(ByteBuffer byteBuffer) {
        return ByteBufferUtil.bytesToHex(byteBuffer);
    }

    /**
     * Returns the hexadecimal {@code String} representation of the specified {@code byte} array.
     *
     * @param bytes A {@code byte} array.
     * @return The hexadecimal {@code String} representation of the specified {@code byte} array.
     */
    public static String toHex(byte[] bytes) {
        return Hex.bytesToHex(bytes);
    }

    /**
     * Returns the hexadecimal {@code String} representation of the specified {@code byte}.
     *
     * @param b A {@code byte}.
     * @return The hexadecimal {@code String} representation of the specified {@code byte}.
     */
    public static String toHex(byte b) {
        return Hex.bytesToHex(b);
    }

    /**
     * Returns the {@link BytesRef} representation of the specified {@link ByteBuffer}.
     *
     * @param bb A {@link ByteBuffer}.
     * @return The {@link BytesRef} representation of the specified {@link ByteBuffer}.
     */
    public static BytesRef bytesRef(ByteBuffer bb) {
        byte[] bytes = asArray(bb);
        return new BytesRef(bytes);
    }

    /**
     * Returns the {@link ByteBuffer} representation of the specified {@link BytesRef}.
     *
     * @param bytesRef A {@link BytesRef}.
     * @return The {@link ByteBuffer} representation of the specified {@link BytesRef}.
     */
    public static ByteBuffer byteBuffer(BytesRef bytesRef) {
        byte[] bytes = bytesRef.bytes;
        return ByteBuffer.wrap(bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);
    }

}