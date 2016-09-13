/*
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
package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for {@link Tracing} avoid testing environment failures.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@SuppressWarnings("unused")
public class Tracer {

    private static final Logger logger = LoggerFactory.getLogger(Tracer.class);

    /**
     * Traces the specified {@code String} message.
     *
     * @param message the message to be traced
     */
    public static void trace(String message) {
        trace(() -> Tracing.trace(message));
    }

    /**
     * Traces the message composed by the specified format and single argument.
     *
     * @param format the message {@code String} format
     * @param arg the argument
     */
    public static void trace(String format, Object arg) {
        trace(() -> Tracing.trace(format, arg));
    }

    /**
     * Traces the message composed by the specified format and arguments pair.
     *
     * @param format the message {@code String} format
     * @param arg1 the first argument
     * @param arg2 the second argument
     */
    public static void trace(String format, Object arg1, Object arg2) {
        trace(() -> Tracing.trace(format, arg1, arg2));
    }

    /**
     * Traces the message composed by the specified format and arguments array.
     *
     * @param format the message {@code String} format
     * @param args the arguments vararg
     */
    public static void trace(String format, Object... args) {
        trace(() -> Tracing.trace(format, args));
    }

    private static void trace(Runnable runnable) {
        try {
            runnable.run();
        } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
            logger.warn("Unable to trace: " + e.getMessage());
        }
    }
}
