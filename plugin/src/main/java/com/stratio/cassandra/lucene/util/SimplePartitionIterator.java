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
package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;

import java.util.Iterator;
import java.util.List;

/**
 * {@link PartitionIterator} composed by a list of {@link SimpleRowIterator}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SimplePartitionIterator implements PartitionIterator {

    private final Iterator<SimpleRowIterator> iterator;

    /**
     * Constructor taking the {@link SimplePartitionIterator} to be iterated.
     *
     * @param rows the rows to be iterated
     */
    public SimplePartitionIterator(List<SimpleRowIterator> rows) {
        iterator = rows.iterator();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        iterator.forEachRemaining(RowIterator::close);
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public RowIterator next() {
        return iterator.next();
    }
}
