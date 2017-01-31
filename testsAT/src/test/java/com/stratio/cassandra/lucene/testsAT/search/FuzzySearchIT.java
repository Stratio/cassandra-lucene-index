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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.fuzzy;

@RunWith(JUnit4.class)
public class FuzzySearchIT extends AbstractSearchIT {

    @Test
    public void testFuzzyAsciiField() {
        filter(fuzzy("ascii_1", "frase tipo asci")).check(2);
    }

    @Test
    public void testFuzzyEmptyAsciiField() {
        filter(fuzzy("ascii_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void testFuzzyAsciiFieldWith1MaxEdits() {
        filter(fuzzy("ascii_1", "frase tipo asci")).check(2);
    }

    @Test
    public void testFuzzyAsciiFieldWith0MaxEdits() {
        filter(fuzzy("ascii_1", "frase tipo asci").maxEdits(0)).check(0);
    }

    @Test
    public void testFuzzyAsciiFieldWith2PrefixLength1() {
        filter(fuzzy("ascii_1", "frase typo ascii").prefixLength(2)).check(1);
    }

    @Test
    public void testFuzzyAsciiFieldWith2PrefixLength2() {
        filter(fuzzy("ascii_1", "phrase tipo ascii").prefixLength(2)).check(0);
    }

    @Test
    public void testFuzzyAsciiFieldWith10MaxExpansions() {
        filter(fuzzy("ascii_1", "frase tipo ascii").maxExpansions(10)).check(2);
    }

    @Test
    public void testFuzzyAsciiFieldWithoutTranspositions() {
        filter(fuzzy("ascii_1", "farse itpo ascii").transpositions(false)).check(0);
    }

    @Test
    public void testFuzzyAsciiFieldWithTranspositions() {
        filter(fuzzy("ascii_1", "farse itpo ascii").transpositions(true)).check(1);
    }

    @Test
    public void testFuzzyAsciiFieldWith5MaxEditsAndTranspositions() {
        filter(fuzzy("ascii_1", "farse itpo ascii").maxEdits(1).transpositions(true)).check(0);
    }

    @Test
    public void testFuzzyInetField() {
        filter(fuzzy("inet_1", "127.0.1.1")).check(4);
    }

    @Test
    public void testFuzzyEmptyInetField() {
        filter(fuzzy("inet_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void testFuzzyInetFieldWith1MaxEdits() {
        filter(fuzzy("inet_1", "127.0.0.1").maxEdits(1)).check(2);
    }

    @Test
    public void testFuzzyInetFieldWith0MaxEdits() {
        filter(fuzzy("inet_1", "127.0.1.1").maxEdits(0)).check(1);
    }

    @Test
    public void testFuzzyInetFieldWith2PrefixLength1() {
        filter(fuzzy("inet_1", "127.0.1.1").prefixLength(2)).check(4);
    }

    @Test
    public void testFuzzyInetFieldWith2PrefixLength2() {
        filter(fuzzy("inet_1", "117.0.1.1").prefixLength(2)).check(0);
    }

    @Test
    public void testFuzzyInetFieldWith10MaxExpansions() {
        filter(fuzzy("inet_1", "127.0.1.1").maxExpansions(10)).check(4);
    }

    @Test
    public void testFuzzyInetFieldWithoutTranspositions() {
        filter(fuzzy("inet_1", "1270..1.1").transpositions(false)).check(3);
    }

    @Test
    public void testFuzzyInetFieldWithTranspositions() {
        filter(fuzzy("inet_1", "1270..1.1").transpositions(true)).check(4);
    }

    @Test
    public void testFuzzyInetFieldWith1MaxEditsAndTranspositions() {
        filter(fuzzy("inet_1", "1270..1.1").maxEdits(1).transpositions(true)).check(1);
    }

    @Test
    public void testFuzzyTextField() {
        filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente")).check(2);
    }

    @Test
    public void testFuzzyEmptyTextField() {
        filter(fuzzy("text_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void testFuzzyTextFieldWith1MaxEdits() {
        filter(fuzzy("text_1", "frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(1)).check(1);
    }

    @Test
    public void testFuzzyTextFieldWith0MaxEdits() {
        filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(0)).check(0);
    }

    @Test
    public void testFuzzyTextFieldWith2PrefixLength1() {
        filter(fuzzy("text_1", "frasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(2)).check(1);
    }

    @Test
    public void testFuzzyTextFieldWith2PrefixLength2() {
        filter(fuzzy("text_1", "rFasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(2)).check(0);
    }

    @Test
    public void testFuzzyTextFieldWith10MaxExpansions() {
        filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxExpansions(10)).check(2);
    }

    @Test
    public void testFuzzyTextFieldWithoutTranspositions() {
        filter(fuzzy("text_1", "Frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(false)).check(0);
    }

    @Test
    public void testFuzzyTextFieldWithTranspositions() {
        filter(fuzzy("text_1", "frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(true)).check(1);
    }

    @Test
    public void testFuzzyTextFieldWith5MaxEditsAndTranspositions() {
        filter(fuzzy("text_1", "Frasseinespacisoconarticulosylaspalabrassuficientes").maxEdits(1)
                                                                                     .transpositions(true)).check(0);
    }

    @Test
    public void testFuzzyVarcharField() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga")).check(3);
    }

    @Test
    public void testFuzzyEmptyVarcharField() {
        filter(fuzzy("varchar_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void testFuzzyVarcharFieldWith1MaxEdits() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(1)).check(2);
    }

    @Test
    public void testFuzzyVarcharFieldWith0MaxEdits() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(0)).check(0);
    }

    @Test
    public void testFuzzyVarcharFieldWith2PrefixLength1() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").prefixLength(2)).check(2);
    }

    @Test
    public void testFuzzyVarcharFieldWith2PrefixLength2() {
        filter(fuzzy("varchar_1", "rfasesencillasnespaciosperomaslarga").prefixLength(2)).check(0);
    }

    @Test
    public void testFuzzyVarcharFieldWith10MaxExpansions() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(10)).check(3);
    }

    @Test
    public void testFuzzyVarcharFieldWithoutTranspositions() {
        filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(false)).check(0);
    }

    @Test
    public void testFuzzyVarcharFieldWithTranspositions() {
        filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(true)).check(2);
    }

    @Test
    public void testFuzzyVarcharFieldWith5MaxEditsAndTranspositions() {
        filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").maxEdits(1).transpositions(true)).check(0);
    }

    @Test
    public void testFuzzyListField1() {
        filter(fuzzy("list_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void testFuzzyListField2() {
        filter(fuzzy("list_1", "l1")).check(5);
    }

    @Test
    public void testFuzzyListField3() {
        filter(fuzzy("list_1", "s1")).check(2);
    }

    @Test
    public void testFuzzyListField4() {
        filter(fuzzy("list_1", "s7l")).check(0);
    }

    @Test
    public void testFuzzySetField1() {
        filter(fuzzy("set_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void testFuzzySetField2() {
        filter(fuzzy("set_1", "l1")).check(2);
    }

    @Test
    public void testFuzzySetField3() {
        filter(fuzzy("set_1", "s1")).check(5);
    }

    @Test
    public void testFuzzySetField4() {
        filter(fuzzy("set_1", "k87")).check(0);
    }

    @Test
    public void testFuzzyMapField1() {
        filter(fuzzy("map_1$k1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void testFuzzyMapField2() {
        filter(fuzzy("map_1$k1", "l1")).check(2);
    }

    @Test
    public void testFuzzyMapField3() {
        filter(fuzzy("map_1$k1", "k1")).check(2);
    }

    @Test
    public void testFuzzyMapField4() {
        filter(fuzzy("map_1$k1", "v1")).check(2);
    }

    @Test
    public void testFuzzyMapFieldTestWithAlias1() {
        filter(fuzzy("string_map$k1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void testFuzzyMapFieldTestWithAlias2() {
        filter(fuzzy("string_map$k1", "l1")).check(2);
    }

    @Test
    public void testFuzzyMapFieldTestWithAlias3() {
        filter(fuzzy("string_map$k1", "k1")).check(2);
    }

    @Test
    public void testFuzzyMapFieldTestWithAlias4() {
        filter(fuzzy("string_map$k1", "v1")).check(2);
    }

}
