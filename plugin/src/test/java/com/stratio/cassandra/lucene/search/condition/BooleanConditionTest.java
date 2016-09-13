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
package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.condition.builder.BooleanConditionBuilder;
import org.apache.lucene.search.BooleanQuery;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.*;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BooleanConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        BooleanConditionBuilder builder = new BooleanConditionBuilder().boost(0.7f)
                                                                       .must(all())
                                                                       .must(all())
                                                                       .should(all(), all())
                                                                       .should(all(), all())
                                                                       .not(all(), all(), all())
                                                                       .not(all(), all(), all());
        BooleanCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Must is not set", 2, condition.must.size());
        assertEquals("Should is not set", 4, condition.should.size());
        assertEquals("Not is not set", 6, condition.not.size());
    }

    @Test
    public void testBuildDefaults() {
        BooleanConditionBuilder builder = new BooleanConditionBuilder();
        BooleanCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertNull("Boost is not set to default", condition.boost);
        assertEquals("Must is not set", 0, condition.must.size());
        assertEquals("Should is not set", 0, condition.should.size());
        assertEquals("Not is not set", 0, condition.not.size());
    }

    @Test
    public void testJsonSerialization() {
        BooleanConditionBuilder builder = new BooleanConditionBuilder().boost(0.7f).must(all());
        testJsonSerialization(builder, "{type:\"boolean\",boost:0.7,must:[{type:\"all\"}],should:[],not:[]}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        BooleanConditionBuilder builder = new BooleanConditionBuilder();
        testJsonSerialization(builder, "{type:\"boolean\",must:[],should:[],not:[]}");
    }

    @Test
    public void testQuery() {
        Schema schema = schema().mapper("name", stringMapper())
                                .mapper("color", stringMapper())
                                .mapper("country", stringMapper())
                                .mapper("age", integerMapper())
                                .defaultAnalyzer("default")
                                .build();
        BooleanCondition condition = bool().must(match("name", "jonathan"), range("age").lower(18).includeLower(true))
                                           .should(match("color", "green"), match("color", "blue"))
                                           .not(match("country", "england"))
                                           .boost(0.4f)
                                           .build();
        BooleanQuery query = condition.doQuery(schema);
        assertEquals("Query count clauses is wrong", 5, query.clauses().size());
    }

    @Test
    public void testQueryEmpty() {
        Schema schema = schema().build();
        BooleanCondition condition = bool().boost(0.4).build();
        BooleanQuery query = condition.doQuery(schema);
        assertEquals("Query count clauses is wrong", 0, query.clauses().size());
    }

    @Test
    public void testQueryPureNot() {
        Schema schema = schema().mapper("name", stringMapper()).build();
        BooleanCondition condition = bool().not(match("name", "jonathan")).boost(0.4).build();
        BooleanQuery query = condition.doQuery(schema);
        assertEquals("Query count clauses is wrong", 2, query.clauses().size());
    }

    @Test
    public void testToString() {
        BooleanCondition condition = bool().must(match("name", "jonathan"), match("age", 18))
                                           .should(match("color", "green"))
                                           .not(match("country", "england").boost(0.7))
                                           .boost(0.5f)
                                           .build();
        assertEquals("Method #toString is wrong",
                     "BooleanCondition{boost=0.5, " +
                     "must=[MatchCondition{boost=null, field=name, value=jonathan, docValues=false}, " +
                     "MatchCondition{boost=null, field=age, value=18, docValues=false}], " +
                     "should=[MatchCondition{boost=null, field=color, value=green, docValues=false}], " +
                     "not=[MatchCondition{boost=0.7, field=country, value=england, docValues=false}]}",
                     condition.toString());
    }

}
