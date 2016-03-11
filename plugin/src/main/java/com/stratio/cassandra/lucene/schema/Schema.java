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

package com.stratio.cassandra.lucene.schema;

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The user-defined mapping from Cassandra columns to Lucene documents.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Schema implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Schema.class);

    /** The {@link Columns} {@link Mapper}s. */
    private final Map<String, Mapper> mappers;

    /** The wrapping all-in-one {@link Analyzer}. */
    private final SchemaAnalyzer analyzer;

    /** The names of the mapped cells. */
    private final Set<String> mappedCells;

    /**
     * Returns a new {@code Schema} for the specified {@link Mapper}s and {@link Analyzer}s.
     *
     * @param defaultAnalyzer the default {@link Analyzer} to be used
     * @param mappers the per field {@link Mapper}s builders to be used
     * @param analyzers the per field {@link Analyzer}s to be used
     */
    public Schema(Analyzer defaultAnalyzer, Map<String, Mapper> mappers, Map<String, Analyzer> analyzers) {
        this.mappers = mappers;
        this.analyzer = new SchemaAnalyzer(defaultAnalyzer, analyzers, mappers);
        mappedCells = mappers.values()
                             .stream()
                             .flatMap(x -> x.mappedColumns.stream())
                             .map(x -> x.contains(Column.UDT_SEPARATOR) ? x.split(Column.UDT_PATTERN)[0] : x)
                             .collect(Collectors.toSet());
    }

    /**
     * Returns the used {@link Analyzer}.
     *
     * @return the used {@link Analyzer}
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * Returns the default {@link Analyzer}.
     *
     * @return the default {@link Analyzer}
     */
    public Analyzer getDefaultAnalyzer() {
        return analyzer.getDefaultAnalyzer().getAnalyzer();
    }

    /**
     * Returns the {@link Analyzer} identified by the specified field name.
     *
     * @param fieldName a field name
     * @return an {@link Analyzer}
     */
    public Analyzer getAnalyzer(String fieldName) {
        return analyzer.getAnalyzer(fieldName).getAnalyzer();
    }

    /**
     * Returns the {@link Mapper} identified by the specified field name, or {@code null} if not found.
     *
     * @param field a field name
     * @return the mapper, or {@code null} if not found.
     */
    public Mapper getMapper(String field) {
        String mapperName = Column.getMapperName(field);
        return mappers.get(mapperName);
    }

    /**
     * Returns the {@link SingleColumnMapper} identified by the specified field name.
     *
     * @param field the field name
     * @return the mapper, or {@code null} if not found
     */
    public SingleColumnMapper getSingleColumnMapper(String field) {
        Mapper mapper = getMapper(field);
        return mapper == null ? null : (SingleColumnMapper) mapper;
    }

    /**
     * Returns the names of the cells mapped by the mappers.
     *
     * @return the names of the mapped cells
     */
    public Set<String> getMappedCells() {
        return mappedCells;
    }

    /**
     * Validates the specified {@link Columns} for mapping.
     *
     * @param columns the {@link Columns} to be validated
     */
    public void validate(Columns columns) {
        for (Mapper mapper : mappers.values()) {
            mapper.validate(columns);
        }
    }

    /**
     * Adds to the specified {@link Document} the Lucene fields representing the specified {@link Columns}.
     *
     * This is done in a best-effort way, so each mapper errors are logged and ignored.
     *
     * @param document the Lucene {@link Document} where the fields are going to be added
     * @param columns the {@link Columns} to be added
     */
    public void addFields(Document document, Columns columns) {
        for (Mapper mapper : mappers.values()) {
            try {
                mapper.addFields(document, columns);
            } catch (IndexException e) {
                logger.error("Error in Lucene index:\n\t" +
                             "while mapping : {}\n\t" +
                             "with mapper   : {}\n\t" +
                             "caused by     : {}", columns, mapper, e.getMessage());
            }
        }
    }

    /**
     * Checks if this is consistent with the specified column family metadata.
     *
     * @param metadata the column family metadata to be validated
     */
    public void validate(CFMetaData metadata) {
        for (Mapper mapper : mappers.values()) {
            mapper.validate(metadata);
        }
    }

    /**
     * Returns if this has any mapping for the specified column definition.
     *
     * @param column the column definition
     * @return {@code true} if there is any mapping for the column, {@code false} otherwise
     */
    public boolean maps(ColumnDefinition column) {
        return mappers.values().stream().anyMatch(mapper -> mapper.maps(column));
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        analyzer.close();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("mappers", mappers).add("analyzer", analyzer).toString();
    }
}
