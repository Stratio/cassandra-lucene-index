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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Utility class for Stratio log messages.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Log {

    private static final Logger log = LoggerFactory.getLogger("stratio");

    public static void info(String message, Object... options) {
        log.info(String.format(message, format(options)));
    }

    public static void info(Throwable throwable, String message, Object... options) {
        log.info(String.format(message, format(options)), throwable);
    }

    public static void debug(String message, Object... options) {
        log.debug(String.format(message, format(options)));
    }

    public static void debug(Throwable throwable, String message, Object... options) {
        log.debug(String.format(message, format(options)), throwable);
    }

    public static void error(String message, Object... options) {
        log.error(String.format(message, format(options)));
    }

    public static void error(Throwable throwable, String message, Object... options) {
        log.error(String.format(message, format(options)), throwable);
    }

    public static void warn(String message, Object... options) {
        log.warn(String.format(message, format(options)));
    }

    public static void warn(Throwable throwable, String message, Object... options) {
        log.warn(String.format(message, format(options)), throwable);
    }

    private static Object[] format(Object... options) {
        Object[] result = new Object[options.length];
        for (int i = 0; i < options.length; i++) {
            Object option = options[i];
            if (option instanceof ByteBuffer) {
                option = ByteBufferUtils.toHex((ByteBuffer) option);
            }
            result[i] = option;
        }
        return result;
    }

}
