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
package com.stratio.cassandra.lucene.testsAT.suite;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.junit.runners.Suite.*;

import com.stratio.cassandra.lucene.testsAT.util.*;

@RunWith(Suite.class)
@SuiteClasses({SearchSuite.class,
               DeletionSuite.class,
               IndexesSuite.class,
               VariaSuite.class,
               StoriesSuite.class,
               BitemporalSuite.class,
               IssuesSuite.class})
public class AllSuite {

    @BeforeClass
    public static void before() {
        CassandraConnection.start();
    }

    @AfterClass
    public static void after() {
        CassandraConnection.stop();
    }

}
