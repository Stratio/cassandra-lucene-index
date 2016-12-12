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
package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.column.Columns;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.util.List;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.integerMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.stringMapper;
import static org.junit.Assert.assertEquals;

/**
 * {@link Mapper} tests.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class MapperTest {

    @Test
    public void testIndexableFields() {
        Mapper mapper = stringMapper().build("f");
        Columns columns = Columns.empty().add("f", "v");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Expected 2 fields", 2, fields.size());
    }

    @Test
    public void testBestEffortIndexableFields() {
        Mapper mapper = integerMapper().build("f");
        Columns columns = Columns.empty().add("f", "1").add("f", "x").add("f", "2");
        List<IndexableField> fields = mapper.bestEffortIndexableFields(columns);
        assertEquals("Expected 4 fields", 4, fields.size());
    }

}
