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
     * @param defaultAnalyzer The default {@link Analyzer} to be used.
     * @param mappers The per field {@link Mapper}s builders to be used.
     * @param analyzers The per field {@link Analyzer}s to be used.
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
     * @return The used {@link Analyzer}.
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * Returns the default {@link Analyzer}.
     *
     * @return The default {@link Analyzer}.
     */
    public Analyzer getDefaultAnalyzer() {
        return analyzer.getDefaultAnalyzer().getAnalyzer();
    }

    /**
     * Returns the {@link Analyzer} identified by the specified field name.
     *
     * @param fieldName A field name.
     * @return The {@link Analyzer} identified by the specified field name.
     */
    public Analyzer getAnalyzer(String fieldName) {
        return analyzer.getAnalyzer(fieldName).getAnalyzer();
    }

    /**
     * Returns the {@link Mapper} identified by the specified field name, or {@code null} if not found.
     *
     * @param field A field name.
     * @return The {@link Mapper} identified by the specified field name, or {@code null} if not found.
     */
    public Mapper getMapper(String field) {
        String mapperName = Column.getMapperName(field);
        return mappers.get(mapperName);
    }

    public SingleColumnMapper getSingleColumnMapper(String field) {
        Mapper mapper = getMapper(field);
        return mapper == null ? null : (SingleColumnMapper) mapper;
    }

    public Set<String> getMappedCells() {
        return mappedCells;
    }

    /**
     * Validates the specified {@link Columns} for mapping.
     *
     * @param columns The {@link Columns} to be validated.
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
     * @param document The Lucene {@link Document} where the fields are going to be added.
     * @param columns The {@link Columns} to be added.
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
     * @param metadata A column family metadata.
     */
    public void validate(CFMetaData metadata) {
        for (Mapper mapper : mappers.values()) {
            mapper.validate(metadata);
        }
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
