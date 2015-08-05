/*
 * Copyright 2015, Stratio.
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

package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.util.Log;

/**
 * {@code RuntimeException} to be thrown when there are schema-related errors.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexException extends RuntimeException {

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param message the detail message.
     * @param args    Arguments referenced by the format specifiers in the format message.
     */
    public IndexException(String message, Object... args) {
        super(String.format(message, args));
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * It also logs  the cause and the message.
     *
     * @param cause   The cause.
     * @param message The detail message.
     * @param args    Arguments referenced by the format specifiers in the format message.
     */
    public IndexException(Throwable cause, String message, Object... args) {
        super(String.format(message, args), cause);
        Log.error(cause, message, args);
    }

    /**
     * Returns this index exception after logging the cause and the message.
     *
     * @return This.
     */
    public IndexException traceError() {
        Log.error(getCause(), getMessage());
        return this;
    }
}
