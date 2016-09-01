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
package com.stratio.cassandra.lucene;

import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

/**
 * {@code RuntimeException} to be thrown when there are Lucene {@link Index}-related errors.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexException extends RuntimeException {

    private static final long serialVersionUID = 2532456234653465436L;

    /**
     * Constructs a new index exception with the specified message.
     *
     * @param message the detail message
     */
    public IndexException(String message) {
        super(message);
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param message the detail message
     * @param arg argument referenced by the format specifier in the format message
     */
    public IndexException(String message, String arg) {
        super(MessageFormatter.format(message, arg).getMessage());
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param message the detail message
     * @param arg1 first argument referenced by the format specifier in the format message
     * @param arg2 second argument referenced by the format specifier in the format message
     */
    public IndexException(String message, String arg1, String arg2) {
        super(MessageFormatter.format(message, arg1, arg2).getMessage());
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param message the detail message
     * @param args arguments referenced by the format specifiers in the format message
     */
    public IndexException(String message, Object... args) {
        super(MessageFormatter.arrayFormat(message, args).getMessage());
    }

    /**
     * Constructs a new index exception with the specified detail message.
     *
     * @param cause the cause
     * @param message the detail message
     */
    public IndexException(Throwable cause, String message) {
        super(message, cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param cause the cause
     * @param message the detail message
     * @param arg argument referenced by the format specifiers in the format message
     */
    public IndexException(Throwable cause, String message, String arg) {
        super(MessageFormatter.format(message, arg).getMessage(), cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param cause the cause
     * @param message the detail message
     * @param arg1 first argument referenced by the format specifiers in the format message
     * @param arg2 first argument referenced by the format specifiers in the format message
     */
    public IndexException(Throwable cause, String message, String arg1, String arg2) {
        super(MessageFormatter.format(message, arg1, arg2).getMessage(), cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param cause the cause
     * @param message the detail message
     * @param args arguments referenced by the format specifiers in the format message
     */
    public IndexException(Throwable cause, String message, Object... args) {
        super(MessageFormatter.arrayFormat(message, args).getMessage(), cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param logger a logger to log the message with ERROR level
     * @param cause the cause
     * @param message the detail message
     */
    public IndexException(Logger logger, Throwable cause, String message) {
        this(cause, message);
        logger.error(getMessage(), cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param logger a logger to log the message with ERROR level
     * @param cause the cause
     * @param message the detail message
     * @param arg argument referenced by the format specifiers in the format message
     */
    public IndexException(Logger logger, Throwable cause, String message, String arg) {
        this(cause, message, arg);
        logger.error(getMessage(), cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param logger a logger to log the message with ERROR level
     * @param cause the cause
     * @param message the detail message
     * @param arg1 first argument referenced by the format specifiers in the format message
     * @param arg2 second argument referenced by the format specifiers in the format message
     */
    public IndexException(Logger logger, Throwable cause, String message, String arg1, String arg2) {
        this(cause, message, arg1, arg2);
        logger.error(getMessage(), cause);
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
        logger.error(getMessage(), cause);
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
