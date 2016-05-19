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
package com.stratio.cassandra.lucene.index;

import org.apache.lucene.search.similarities.ClassicSimilarity;

/**
 * {@link ClassicSimilarity} that ignores the inverse document frequency, doing the similarity independent of the index
 * context.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class NoIDFSimilarity extends ClassicSimilarity {

    /**
     * Returns a constant neutral score value of {@code 1.0}.
     */
    @Override
    public float idf(long docFreq, long numDocs) {
        return 1.0f;
    }
}
