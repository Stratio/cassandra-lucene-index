/**
 * Copyright (C) 2016 Stratio (http://stratio.com)
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
package com.stratio.cassandra.lucene.search.sort;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SortTest {

    @Test
    public void testBuildEmpty() {
        List<SortField> emptyList = new ArrayList<>();
        Sort sort = new Sort(emptyList);
        assertEquals("Sort build with empty SortField List must return an empty list", sort.getSortFields().size(), 0);
    }

    @Test
    public void testBuildDefaults() {
        SortField sortField = new SimpleSortField("field", null);
        List<SortField> oneElementList = new ArrayList<>();
        oneElementList.add(sortField);
        Sort sort = new Sort(oneElementList);
        List<SortField> result = sort.getSortFields();
        assertEquals("Sort build with one element SortField List must return a list with size 1", result.size(), 1);
        assertEquals("Sort build with 1 sort Field does not returns the same SortField ", sortField, result.get(0));

    }

    @Test
    public void testBuild2() {
        SimpleSortField sortField = new SimpleSortField("field", false);
        SimpleSortField sortField2 = new SimpleSortField("field_2", true);
        List<SortField> twoElementList = new ArrayList<>();
        twoElementList.add(sortField);
        twoElementList.add(sortField2);
        Sort sort = new Sort(twoElementList);
        List<SortField> result = sort.getSortFields();
        assertEquals("Sort build with two elements SortField List must return a list with size 2", result.size(), 2);
        assertEquals("Sort build with two SortField does not returns the same SortFields ", twoElementList, result);

    }

    @Test
    public void testIterator() {
        SimpleSortField sortField = new SimpleSortField("field", false);
        SimpleSortField sortField2 = new SimpleSortField("field_2", true);
        SimpleSortField sortField3 = new SimpleSortField("field_3", true);
        List<SortField> sortFields = new ArrayList<>();
        sortFields.add(sortField);
        sortFields.add(sortField2);
        sortFields.add(sortField3);
        Sort sort = new Sort(sortFields);
        int numElems = 0;
        for (SortField sortF : sort) {
            numElems += 1;
            assertFalse("Sort iterator not returning all the sortFields",
                        !sortF.equals(sortField) && !sortF.equals(sortField2) && !sortF.equals(sortField3));
        }
        assertEquals("Sort build with 3 elements iterator must return 3 elems", numElems, 3);
    }
}