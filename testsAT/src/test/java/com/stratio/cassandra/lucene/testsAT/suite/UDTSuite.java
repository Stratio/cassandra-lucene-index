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

import com.stratio.cassandra.lucene.testsAT.udt.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({CheckNonFrozenUDTAT.class,
                     TupleIndexingAT.class,
                     UDTCollectionsAT.class,
                     UDTIndexingAT.class,
                     UDTPartialInsertsAT.class,
                     UDTValidationAT.class,})
public class UDTSuite {
}
