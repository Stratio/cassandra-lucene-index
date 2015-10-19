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

package com.stratio.cassandra.lucene.service;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class NoIDFSimilarityTest {

    @Test
    public void testIDFSimilarity() {
        NoIDFSimilarity noIDFSimilarity = new NoIDFSimilarity();

        assertEquals("NoIDFSimilarity must always return 1.0f", noIDFSimilarity.idf(0l, 0l), 1.0f);
        assertEquals("NoIDFSimilarity must always return 1.0f", noIDFSimilarity.idf(1l, 5l), 1.0f);
        assertEquals("NoIDFSimilarity must always return 1.0f", noIDFSimilarity.idf(10000l, 10943l), 1.0f);
        assertEquals("NoIDFSimilarity must always return 1.0f", noIDFSimilarity.idf(-45667l, 2132189l), 1.0f);
        assertEquals("NoIDFSimilarity must always return 1.0f", noIDFSimilarity.idf(367423794l, -394612l), 1.0f);
        assertEquals("NoIDFSimilarity must always return 1.0f", noIDFSimilarity.idf(-2147294213l, 15264214l), 1.0f);
    }

}
