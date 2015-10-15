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

import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

/**
 * Tests for {@link TimeCounter}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TimeCounterTest {

    @Test
    public void testFlow() throws InterruptedException {
        TimeCounter tc = TimeCounter.create();
        tc.start();
        Thread.sleep(10);
        tc.stop();
        assertTrue("TimeCounter milliseconds should be greater or equal that 10", tc.getTime() >= 10);
        assertTrue("TimeCounter milliseconds should be greater or equal that 10", tc.getNanoTime() >= 10000);
        assertNotNull(tc.toString());
    }

    @Test(expected = IllegalStateException.class)
    public void testStartStarted() {
        TimeCounter tc = TimeCounter.create();
        tc.start();
        tc.start();
    }

    @Test
    public void testStartStopped() throws InterruptedException {
        TimeCounter tc = TimeCounter.create();
        tc.start();
        Thread.sleep(5);
        tc.stop();
        long t1 = tc.getTime();
        tc.start();
        Thread.sleep(5);
        tc.stop();
        long t2 = tc.getTime();
        assertTrue("TimeCounter milliseconds should be incremented", t2 > t1);
    }

    @Test(expected = IllegalStateException.class)
    public void testStopStopped() {
        TimeCounter tc = TimeCounter.create();
        tc.start();
        tc.stop();
        tc.stop();
    }

    @Test(expected = IllegalStateException.class)
    public void testStopNotStarted() {
        TimeCounter tc = TimeCounter.create();
        tc.stop();
    }
}
