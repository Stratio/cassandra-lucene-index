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
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.PhraseCondition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class PhraseConditionBuilderTest {

    @Test
    public void testBuildList() {
        String[] values = new String[]{"value1", "value2"};
        PhraseConditionBuilder builder = new PhraseConditionBuilder("field", values);
        builder.slop(2);
        PhraseCondition condition = builder.build();
        Assert.assertNotNull(condition);
        Assert.assertEquals("field", condition.getField());
        Assert.assertArrayEquals(values, condition.getValues());
        Assert.assertEquals(2, condition.getSlop());
    }

    @Test
    public void testBuildArray() {
        List<String> values = Arrays.asList("value1", "value2");
        PhraseConditionBuilder builder = new PhraseConditionBuilder("field", values);
        builder.slop(2);
        PhraseCondition condition = builder.build();
        Assert.assertNotNull(condition);
        Assert.assertEquals("field", condition.getField());
        Assert.assertArrayEquals(values.toArray(), condition.getValues());
        Assert.assertEquals(2, condition.getSlop());
    }
}
