/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.schema.column;

import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A cell of a CQL3 logic {@link ColumnBuilder}, which in most cases is different from a storage engine column.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ColumnBuilder {

    private final String cellName;
    private final List<String> udtNames;
    private final List<String> mapNames;
    private boolean isMultiCell = false;

    public ColumnBuilder(String cellName) {
        this.cellName = cellName;
        udtNames = new ArrayList<>();
        mapNames = new ArrayList<>();
    }

    public <T> Column<T> composedValue(T composedValue, AbstractType<T> type) {
        ByteBuffer decomposedValue = type.decompose(composedValue);
        return new Column<>(cellName, udtNames, mapNames, decomposedValue, composedValue, type, isMultiCell);
    }

    public <T> Column<T> decomposedValue(ByteBuffer decomposedValue, AbstractType<T> type) {
        T composedValue = type.compose(decomposedValue);
        return new Column<>(cellName, udtNames, mapNames, decomposedValue, composedValue, type, isMultiCell);
    }

    public ColumnBuilder multiCell(boolean isMultiCell) {
        this.isMultiCell = isMultiCell;
        return this;
    }

    public ColumnBuilder udtName(String name) {
        udtNames.add(name);
        return this;
    }

    public ColumnBuilder mapName(String name) {
        mapNames.add(name);
        return this;
    }

    @Override
    public ColumnBuilder clone() {
        ColumnBuilder clone = new ColumnBuilder(cellName);
        clone.isMultiCell = isMultiCell;
        for (String udtName : udtNames) {
            clone.udtNames.add(udtName);
        }
        for (String mapName : mapNames) {
            clone.mapNames.add(mapName);
        }
        return clone;
    }
}
