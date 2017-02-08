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
package com.stratio.cassandra.lucene.testsAT;

import com.stratio.cassandra.lucene.testsAT.util.CassandraConnection;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BaseIT {

    public static final Logger logger = LoggerFactory.getLogger("TEST");

    @BeforeClass
    public static void connect() {
        CassandraConnection.connect();
    }

    private <T> void assertPure(String msg, int count, T expected, Callable<T> callable) throws Exception {
        if (count > 0) {
            T actual = callable.call();
            Assert.assertEquals(msg, expected, actual);
            assertPure(msg, count - 1, actual, callable);
        }
    }

    protected <T> void assertPure(String msg, Callable<T> callable) throws Exception {
        assertPure(msg, 10, callable.call(), callable);
    }
}
