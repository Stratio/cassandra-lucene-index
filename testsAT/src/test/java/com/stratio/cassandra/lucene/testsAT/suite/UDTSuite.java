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

package com.stratio.cassandra.lucene.testsAT.suite;

import com.stratio.cassandra.lucene.testsAT.udt.CheckNonFrozenUDTTest;
import com.stratio.cassandra.lucene.testsAT.udt.UDTCollectionsTest;
import com.stratio.cassandra.lucene.testsAT.udt.UDTIndexingTest;
import com.stratio.cassandra.lucene.testsAT.udt.UDTValidationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({UDTValidationTest.class,
                     UDTIndexingTest.class,
                     UDTCollectionsTest.class,
                     CheckNonFrozenUDTTest.class})
public class UDTSuite {

}
