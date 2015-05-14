/*
 * Copyright 2015, Stratio.
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
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInteger;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import junit.framework.Assert;
import org.apache.lucene.search.BooleanQuery;
import org.junit.Test;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class BooleanConditionTest extends AbstractConditionTest {

    @Test
    public void testJson() {
        testJsonCondition(query(bool().must(match("name", "jonathan"), range("age").lower(18).includeLower(true))
                                      .should(match("color", "green"), match("color", "blue"))
                                      .not(match("country", "england"))
                                      .boost(0.5f)).filter(match("section", "customers")));
    }

    @Test
    public void testQuery() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.DEFAULT.get());
        when(schema.getMapperSingle("name")).thenReturn(new ColumnMapperString(null, null, null));
        when(schema.getMapperSingle("color")).thenReturn(new ColumnMapperString(null, null, null));
        when(schema.getMapperSingle("country")).thenReturn(new ColumnMapperString(null, null, null));
        when(schema.getMapperSingle("age")).thenReturn(new ColumnMapperInteger(null, null, null));
        BooleanCondition condition = bool().must(match("name", "jonathan"), range("age").lower(18).includeLower(true))
                                           .should(match("color", "green"), match("color", "blue"))
                                           .not(match("country", "england"))
                                           .boost(0.4f)
                                           .build();
        BooleanQuery query = (BooleanQuery) condition.query(schema);
        Assert.assertEquals(5, query.getClauses().length);
        Assert.assertEquals(0.4f, query.getBoost());
    }

}
