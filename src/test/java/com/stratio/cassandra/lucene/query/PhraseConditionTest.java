/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.query;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.filter;
import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.phrase;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class PhraseConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        String[] values = new String[]{"hello", "adios"};
        PhraseCondition condition = new PhraseCondition(0.5f, "name", values, 2);
        assertEquals(0.5f, condition.getBoost(), 0);
        assertEquals("name", condition.getField());
        assertArrayEquals(values, condition.getValues());
        assertEquals(2, condition.getSlop());
    }

    @Test
    public void testBuildDefaults() {
        String[] values = new String[]{"hello", "adios"};
        PhraseCondition condition = new PhraseCondition(null, "name", values, null);
        assertEquals(PhraseCondition.DEFAULT_BOOST, condition.getBoost(), 0);
        assertEquals("name", condition.getField());
        assertArrayEquals(values, condition.getValues());
        assertEquals(PhraseCondition.DEFAULT_SLOP, condition.getSlop());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildNullValues() {
        new PhraseCondition(null, "name", null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildNegativeSlop() {
        String[] values = new String[]{"hello", "adios"};
        new PhraseCondition(null, "name", values, -1);
    }

    @Test
    public void testPhraseQuery() {

        Schema schema = mockSchema("name", new ColumnMapperString("name", true, true, null));

        String[] values = new String[]{"hola", "adios", "the", "a"};
        PhraseCondition condition = new PhraseCondition(0.5f, "name", values, 2);
        Query query = condition.query(schema);
        assertNotNull(query);
        assertEquals(PhraseQuery.class, query.getClass());
        PhraseQuery luceneQuery = (PhraseQuery) query;
        assertEquals(values.length, luceneQuery.getTerms().length);
        assertEquals(2, luceneQuery.getSlop());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testJson() {
        testJsonCondition(filter(phrase("name", "hola", "adios").slop(1).boost(0.5f)));
    }

    @Test
    public void testToString() {
        PhraseCondition condition = new PhraseCondition(0.5f, "name", new String[]{"hola", "adios"}, 2);
        assertEquals("PhraseCondition{boost=0.5, field=name, values=[hola, adios], slop=2}", condition.toString());
    }

}
