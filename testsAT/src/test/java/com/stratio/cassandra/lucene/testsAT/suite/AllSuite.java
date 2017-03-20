/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
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

import com.stratio.cassandra.lucene.testsAT.util.CassandraConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({SearchSuite.class,
               DeletionSuite.class,
               IndexesSuite.class,
               VariaSuite.class,
               StoriesSuite.class,
               IssuesSuite.class,
               UDTSuite.class})
public class AllSuite {

    @BeforeClass
    public static void before() {
        CassandraConnection.connect();
    }

    @AfterClass
    public static void after() {
        CassandraConnection.disconnect();
    }

}
