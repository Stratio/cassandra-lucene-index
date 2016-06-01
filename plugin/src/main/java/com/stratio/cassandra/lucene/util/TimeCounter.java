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

/**
 * Class for measuring time durations.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class TimeCounter {

    private enum State {
        UNSTARTED, RUNNING, STOPPED
    }

    private long startTimeMillis, stopTimeMillis;
    private State state;

    /**
     * Returns a new stopped {@link TimeCounter}.
     *
     * @return a new stopped counter
     */
    public static TimeCounter create() {
        return new TimeCounter();
    }

    /**
     * Builds a new stopped {@link TimeCounter}.
     */
    private TimeCounter() {
        this.state = State.UNSTARTED;
    }

    /**
     * Starts or resumes the time count.
     *
     * @return this
     */
    public TimeCounter start() {
        switch (state) {
            case UNSTARTED:
                startTimeMillis = System.currentTimeMillis();
                break;
            case RUNNING:
                throw new IllegalStateException("Already started");
            case STOPPED:
                startTimeMillis += System.currentTimeMillis() - stopTimeMillis;
                break;
            default:
                throw new IllegalStateException("Unrecognized state " + state);
        }
        state = State.RUNNING;
        return this;
    }

    /**
     * Stops or suspends the time count.
     *
     * @return this
     */
    public TimeCounter stop() {
        switch (state) {
            case UNSTARTED:
                throw new IllegalStateException("Not started. ");
            case STOPPED:
                throw new IllegalStateException("Already stopped. ");
            case RUNNING:
                this.stopTimeMillis = System.currentTimeMillis();
            default:
                state = State.STOPPED;
                return this;
        }
    }

    /**
     * Returns a summary of the time that the stopwatch has recorded as a string.
     *
     * @return a summary of the time that the stopwatch has recorded
     */
    public String toString() {
        return String.valueOf(stopTimeMillis - startTimeMillis) + " ms";
    }

    /**
     * Returns the counted time in milliseconds.
     *
     * @return the counted time in milliseconds
     */
    public long getTime() {
        return stopTimeMillis - startTimeMillis;
    }
}
