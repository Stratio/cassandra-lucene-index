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
package com.stratio.cassandra.lucene.testsAT.search;

import com.datastax.driver.core.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class BooleanSearchAT extends AbstractSearchAT {

    @Test
    public void testBooleanFilterEmpty() {
        filter(bool()).check(0);
    }

    @Test
    public void testBooleanFilterNot() {
        filter(bool().not(match("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51c"))).check(4);
    }

    @Test
    public void testBooleanFilterMust() {
        filter(bool().must(wildcard("ascii_1", "frase*")).must(wildcard("inet_1", "127.0.*"))).check(2);
    }

    @Test
    public void testBooleanFilterShould() {
        filter(bool().should(wildcard("ascii_1", "frase*")).should(wildcard("inet_1", "127.0.*"))).check(4);
    }

    @Test
    public void testBooleanFilterMustAndNot() {
        filter(bool().must(wildcard("ascii_1", "frase*"))
                     .must(wildcard("inet_1", "127.0.*"))
                     .not(match("inet_1", "127.0.0.1"))).check(1);
    }

    @Test
    public void testBooleanFilterShouldAndNot() {
        filter(bool().should(wildcard("ascii_1", "frase*"), wildcard("inet_1", "127.0.*"))
                     .not(match("inet_1", "127.0.0.1"))).check(3);
    }

    @Test
    public void testBooleanFilterWithBoost() {

        List<Row> firstRows = filter(bool().must(fuzzy("inet_1", "127.1.1.1").boost(0.9))
                                           .must(fuzzy("inet_1", "127.1.0.1").boost(0.1))
                                           .not(match("integer_1", 1), match("integer_1", -4))).get();
        assertEquals("Expected 3 results!", 3, firstRows.size());

        List<Row> secondRows = filter(bool().must(fuzzy("inet_1", "127.1.1.1").boost(0.0))
                                            .must(fuzzy("inet_1", "127.1.0.1").boost(0.9))
                                            .not(match("integer_1", 1), match("integer_1", -4))).get();
        assertEquals("Expected 3 results!", 3, secondRows.size());

        assertEquals("Expected same number of results ", firstRows.size(), secondRows.size());
        boolean equals = true;
        for (int i = 0; i < firstRows.size(); i++) {
            Integer firstResult = firstRows.get(i).getInt("integer_1");
            Integer secondResult = secondRows.get(i).getInt("integer_1");
            equals &= firstResult.equals(secondResult);
        }
        assertTrue("Expected same sorting!", equals);
    }

    @Test
    public void testBooleanQueryEmpty() {
        filter(bool()).check(0);
    }

    @Test
    public void testBooleanQueryNot() {
        filter(bool().not(match("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51c"))).check(4);
    }

    @Test
    public void testBooleanQueryMust() {
        filter(bool().must(wildcard("ascii_1", "frase*")).must(wildcard("inet_1", "127.0.*"))).check(2);
    }

    @Test
    public void testBooleanQueryShould() {
        filter(bool().should(wildcard("ascii_1", "frase*")).should(wildcard("inet_1", "127.0.*"))).check(4);
    }

    @Test
    public void testBooleanQueryMustAndNot() {
        filter(bool().must(wildcard("ascii_1", "frase*"))
                     .must(wildcard("inet_1", "127.0.*"))
                     .not(match("inet_1", "127.0.0.1"))).check(1);
    }

    @Test
    public void testBooleanQueryShouldAndNot() {
        filter(bool().should(wildcard("ascii_1", "frase*"), wildcard("inet_1", "127.0.*"))
                     .not(match("inet_1", "127.0.0.1"))).check(3);
    }

}
