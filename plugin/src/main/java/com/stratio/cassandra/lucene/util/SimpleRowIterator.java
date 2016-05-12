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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

/**
 * {@link RowIterator} representing a single CQL {@link Row}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SimpleRowIterator implements RowIterator {

    private final CFMetaData metadata;
    private final DecoratedKey key;
    private final PartitionColumns columns;
    private final Row staticRow;
    private final Row row;
    private final Iterator<Row> rows;
    private Function<Row, Row> decorator;

    /**
     * Builds a new {@link SimpleRowIterator} from the current position of the specified {@link RowIterator}. Any other
     * rows in the specified iterator won't be read.
     *
     * @param iterator the {@link Row} iterator
     */
    public SimpleRowIterator(RowIterator iterator) {
        this.metadata = iterator.metadata();
        this.key = iterator.partitionKey();
        this.columns = iterator.columns();
        this.staticRow = iterator.staticRow();
        this.row = iterator.next();
        this.rows = Collections.singletonList(row).iterator();
    }

    public void setDecorator(Function<Row, Row> decorator) {
        this.decorator = decorator;
    }

    /** {@inheritDoc} */
    @Override
    public CFMetaData metadata() {
        return metadata;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReverseOrder() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public PartitionColumns columns() {
        return columns;
    }

    /** {@inheritDoc} */
    @Override
    public DecoratedKey partitionKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override
    public Row staticRow() {
        return staticRow;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {

    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return rows.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public Row next() {
        Row row = rows.next();
        return decorator == null ? row : decorator.apply(row);
    }

    /**
     * Returns the decorated {@link Row}.
     *
     * @return the decorated {@link Row}
     */
    public Row getRow() {
        return row;
    }
}
