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
package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.schema.Columns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperTest {

    @Test
    public void testSupportsSimple() {
        testSupports(true, UTF8Type.instance, UTF8Type.instance);
    }

    @Test
    public void testSupportsSimpleNot() {
        testSupports(false, UTF8Type.instance, IntegerType.instance);
    }

    @Test
    public void testSupportsMultiple() {
        testSupports(true, UTF8Type.instance, UTF8Type.instance, IntegerType.instance);
    }

    @Test
    public void testSupportsMultipleNot() {
        testSupports(false, UUIDType.instance, UTF8Type.instance, IntegerType.instance);
    }

    @Test
    public void testSupportsMap() {
        testSupports(true, MapType.getInstance(IntegerType.instance, UTF8Type.instance, false), UTF8Type.instance);
    }

    @Test
    public void testSupportsMapMultiCell() {
        testSupports(true, MapType.getInstance(IntegerType.instance, UTF8Type.instance, true), UTF8Type.instance);
    }

    @Test
    public void testSupportsMapNot() {
        testSupports(false, MapType.getInstance(IntegerType.instance, UTF8Type.instance, false), IntegerType.instance);
    }

    @Test
    public void testSupportsList() {
        testSupports(true, ListType.getInstance(UTF8Type.instance, false), UTF8Type.instance);
    }

    @Test
    public void testSupportsListMultiCell() {
        testSupports(true, ListType.getInstance(UTF8Type.instance, true), UTF8Type.instance);
    }

    @Test
    public void testSupportsListNot() {
        testSupports(false, ListType.getInstance(UTF8Type.instance, false), IntegerType.instance);
    }

    @Test
    public void testSupportsSet() {
        testSupports(true, SetType.getInstance(UTF8Type.instance, false), UTF8Type.instance);
    }

    @Test
    public void testSupportsSetMultiCell() {
        testSupports(true, SetType.getInstance(UTF8Type.instance, true), UTF8Type.instance);
    }

    @Test
    public void testSupportsSetNot() {
        testSupports(false, SetType.getInstance(IntegerType.instance, false), UTF8Type.instance);
    }

    @Test
    public void testSupportsReversed() {
        testSupports(true, ReversedType.getInstance(UTF8Type.instance), UTF8Type.instance);
    }

    private void testSupports(boolean expected, AbstractType<?> candidateType, AbstractType<?>... supportedTypes) {

        ColumnMapper mapper = new ColumnMapper(null, null, supportedTypes) {
            @Override
            public void addFields(Document document, Columns columns) {

            }

            @Override
            public SortField sortField(boolean reverse) {
                return null;
            }
        };
        assertEquals(expected, mapper.supports(candidateType));
    }

}
