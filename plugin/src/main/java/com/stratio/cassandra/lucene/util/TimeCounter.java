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

import org.apache.commons.lang3.time.StopWatch;

/**
 * Class for measuring time durations.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class TimeCounter {

    private enum State {
        UNSTARTED, RUNNING, STOPPED
    }

    private final StopWatch watch;
    private State state;

    /**
     * Returns a new stopped {@link TimeCounter}.
     *
     * @return A new stopped {@link TimeCounter}.
     */
    public static TimeCounter create() {
        return new TimeCounter();
    }

    /**
     * Builds a new stopped {@link TimeCounter}.
     */
    private TimeCounter() {
        this.watch = new StopWatch();
        this.state = State.UNSTARTED;
    }

    /**
     * Starts or resumes the time count.
     *
     * @return This.
     */
    public TimeCounter start() {
        switch (state) {
            case UNSTARTED:
                watch.start();
                break;
            case RUNNING:
                throw new IllegalStateException("Already started. ");
            case STOPPED:
                watch.resume();
        }
        state = State.RUNNING;
        return this;
    }

    /**
     * Stops or suspends the time count.
     *
     * @return This.
     */
    public TimeCounter stop() {
        switch (state) {
            case UNSTARTED:
                throw new IllegalStateException("Not started. ");
            case STOPPED:
                throw new IllegalStateException("Already stopped. ");
            case RUNNING:
                watch.suspend();
        }
        state = State.STOPPED;
        return this;
    }

    /**
     * Returns a summary of the time that the stopwatch recorded as a string.
     *
     * @return A summary of the time that the stopwatch recorded as a string.
     */
    public String toString() {
        return watch.toString();
    }

    /**
     * Returns the counted time in milliseconds.
     *
     * @return The counted time in milliseconds.
     */
    public long getTime() {
        return watch.getTime();
    }

    /**
     * Returns the counted time in nanoseconds.
     *
     * @return The counted time in nanoseconds.
     */
    public long getNanoTime() {
        return watch.getNanoTime();
    }
}
