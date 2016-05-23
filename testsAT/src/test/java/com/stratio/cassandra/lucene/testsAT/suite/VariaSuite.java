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

import com.stratio.cassandra.lucene.testsAT.varia.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({AllowFilteringWith1000SimilarRowsAT.class,
               AllowFilteringWith1000MixedRowsAT.class,
               InOperatorWithSkinnyRowsAT.class,
               InOperatorWithWideRowsAT.class,
               TokenRangeWithSkinnyRowsAT.class,
               TokenRangeWithWideRowsMultiPartitionAT.class,
               TokenRangeWithWideRowsMultiClusteringAT.class,
               SearchWithLongWideRowsAT.class,
               ReadStaticColumnsAT.class,
               UDFsAT.class,
               BoundStatementWitTopKQuery.class,
               StatelessSearchWithSkinnyRowsAT.class,
               StatelessSearchWithWideRowsAT.class,
               SelectPartialExpiredTTLSkinnyRows.class,
               SelectTotalExpiredTTLSkinnyRows.class,
               SelectPartialExpiredTTLWideRows.class,
               SelectTotalExpiredTTLWideRows.class})
public class VariaSuite {
}
