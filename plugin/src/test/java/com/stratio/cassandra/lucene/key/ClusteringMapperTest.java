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
package com.stratio.cassandra.lucene.key;

import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link TokenMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ClusteringMapperTest {

    private static final List<Token> TOKENS = Arrays.asList(new LongToken(Long.MIN_VALUE),
                                                            new LongToken(-12345L),
                                                            new LongToken(-123L),
                                                            new LongToken(-2L),
                                                            new LongToken(-1L),
                                                            new LongToken(0L),
                                                            new LongToken(1L),
                                                            new LongToken(2L),
                                                            new LongToken(123L),
                                                            new LongToken(12345L),
                                                            new LongToken(Long.MAX_VALUE));
}
