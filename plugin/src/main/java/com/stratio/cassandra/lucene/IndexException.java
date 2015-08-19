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

/**
 * {@code RuntimeException} to be thrown when there are schema-related errors.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexException extends RuntimeException {

    private static final long serialVersionUID = 2532456234653465436L;

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
     * @param cause   The cause.
     * @param message The detail message.
     * @param args    Arguments referenced by the format specifiers in the format message.
     */
    public IndexException(Throwable cause, String message, Object... args) {
        super(String.format(message, args), cause);
    }

    /**
     * Constructs a new index exception with the specified cause.
     *
     * @param cause The cause.
     */
    public IndexException(Throwable cause) {
        super(cause);
    }

}
