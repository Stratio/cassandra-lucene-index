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

package com.stratio.cassandra.lucene;

import org.slf4j.Logger;

/**
 * {@code RuntimeException} to be thrown when there are Lucene {@link Index}-related errors.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexException extends RuntimeException {

    private static final long serialVersionUID = 2532456234653465436L;

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param message the detail message.
     * @param args arguments referenced by the format specifiers in the format message
     */
    public IndexException(String message, Object... args) {
        super(String.format(message, args));
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param cause the cause
     * @param message the detail message
     * @param args arguments referenced by the format specifiers in the format message
     */
    public IndexException(Throwable cause, String message, Object... args) {
        super(String.format(message, args), cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param logger a logger to log the message with ERROR level
     * @param cause the cause
     * @param message the detail message
     * @param args arguments referenced by the format specifiers in the format message
     */
    public IndexException(Logger logger, Throwable cause, String message, Object... args) {
        this(cause, message, args);
        logger.error(getMessage());
    }

    /**
     * Constructs a new index exception with the specified cause.
     *
     * @param cause the cause
     */
    public IndexException(Throwable cause) {
        super(cause);
    }

}
