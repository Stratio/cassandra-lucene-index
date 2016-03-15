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

package com.stratio.cassandra.lucene.column;

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

    /**
     * Constructor taking the cell name.
     *
     * @param cellName the cell name
     */
    public ColumnBuilder(String cellName) {
        this.cellName = cellName;
        udtNames = new ArrayList<>();
        mapNames = new ArrayList<>();
    }

    /**
     * Returns a new {@link Column} using the specified composed value and its type.
     *
     * @param composedValue the decomposed value
     * @param type the value type
     * @param <T> the marshaller's base type
     * @return the built column
     */
    public <T> Column<T> buildWithComposed(T composedValue, AbstractType<T> type) {
        ByteBuffer decomposedValue = type.decompose(composedValue);
        return new Column<>(cellName, udtNames, mapNames, decomposedValue, composedValue, type);
    }

    /**
     * Returns a new {@link Column} using the specified decomposed value and its type.
     *
     * @param decomposedValue the decomposed value
     * @param type the value type
     * @param <T> the marshaller's base type
     * @return the built column
     */
    public <T> Column<T> buildWithDecomposed(ByteBuffer decomposedValue, AbstractType<T> type) {
        T composedValue = type.compose(decomposedValue);
        return new Column<>(cellName, udtNames, mapNames, decomposedValue, composedValue, type);
    }

    /**
     * Returns this builder with the specified UDT name component.
     *
     * @param name the UDT name component
     * @return this
     */
    public ColumnBuilder withUDTName(String name) {
        ColumnBuilder clone = copy();
        clone.udtNames.add(name);
        return clone;
    }

    /**
     * Returns this builder with the specified map name component.
     *
     * @param name the map key name component
     * @return this
     */
    public ColumnBuilder withMapName(String name) {
        ColumnBuilder clone = copy();
        clone.mapNames.add(name);
        return clone;
    }

    /**
     * Returns a new copy of this.
     *
     * @return the copy
     */
    public ColumnBuilder copy() {
        ColumnBuilder clone = new ColumnBuilder(cellName);
        clone.udtNames.addAll(udtNames);
        clone.mapNames.addAll(mapNames);
        return clone;
    }
}
