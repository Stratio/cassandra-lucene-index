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

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Token;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TokenMapperTest {

    private static final List<Token> TOKENS = Arrays.asList(new LongToken(Long.MIN_VALUE),
                                                            new LongToken(-12345),
                                                            new LongToken(-123),
                                                            new LongToken(0),
                                                            new LongToken(123),
                                                            new LongToken(12345),
                                                            new LongToken(Long.MAX_VALUE));

    private static final List<String> STRINGS = Arrays.asList("0000000000000000",
                                                              "7fffffffffffcfc7",
                                                              "7fffffffffffff85",
                                                              "8000000000000000",
                                                              "800000000000007b",
                                                              "8000000000003039",
                                                              "ffffffffffffffff");

    @Test
    public void testToCollated() {
        List<String> strings = TOKENS.stream()
                                     .map(TokenMapper::toCollated)
                                     .map(UTF8Type.instance::compose)
                                     .collect(Collectors.toList());
        assertArrayEquals("TokenMapper.toCollated is wrong", STRINGS.toArray(), strings.toArray());
    }

    @Test
    public void testFromCollated() {
        List<Token> tokens = STRINGS.stream()
                                    .map(UTF8Type.instance::decompose)
                                    .map(TokenMapper::fromCollated)
                                    .collect(Collectors.toList());
        assertArrayEquals("TokenMapper.toCollated is wrong", TOKENS.toArray(), tokens.toArray());
    }
}
