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
package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;

/**
 * Class for mapping several binary types from/to {@link String} using a base of 256 UTF-8 characters.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
class Base256Serializer {

    /**
     * Returns the {@code char} array representation of the specified {@code byte} array.
     *
     * @param bytes The {@code byte} array to be converted.
     * @return The {@code char} array representation of the specified {@code byte} array.
     */
    private static char[] chars(byte[] bytes) {
        char[] chars = new char[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            int pos = bytes[i] & 0xff;
            chars[i] = (char) pos;
        }
        return chars;
    }

    /**
     * Returns the {@code byte} array representation of the specified {@code char} array.
     *
     * @param chars The {@code char} array to be converted.
     * @return The {@code byte} array representation of the specified {@code char} array.
     */
    private static byte[] bytes(char[] chars) {
        byte[] bytes = new byte[chars.length];
        for (int i = 0; i < bytes.length; i++) {
            char c = chars[i];
            bytes[i] = (byte) c;
        }
        return bytes;
    }

    /**
     * Returns the {@code byte} array representation of the specified {@code String}.
     *
     * @param string The {@code String} to be converted.
     * @return The {@code byte} array representation of the specified {@code String}.
     */
    private static byte[] bytes(String string) {
        return bytes(string.toCharArray());
    }

    /**
     * Returns the {@code String} representation of the specified {@code ByteBuffer}.
     *
     * @param byteBuffer The {@code ByteBuffer} to be converted.
     * @return The {@code String} representation of the specified {@code ByteBuffer}.
     */
    public static String string(ByteBuffer byteBuffer) {
        ByteBuffer bb = ByteBufferUtil.clone(byteBuffer);
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return new String(chars(bytes));
    }

    /**
     * Returns the {@code ByteBuffer} representation of the specified {@code String}.
     *
     * @param string The {@code String} to be converted.
     * @return The {@code ByteBuffer} representation of the specified {@code String}.
     */
    public static ByteBuffer byteBuffer(String string) {
        return ByteBuffer.wrap(bytes(string));
    }
}
