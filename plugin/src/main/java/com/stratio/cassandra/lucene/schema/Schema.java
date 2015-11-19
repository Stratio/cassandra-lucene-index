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

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    /** The names of the mapped columns. */
    private final Set<String> mappedColumns;

    /**
     * Returns a new {@code Schema} for the specified {@link Mapper}s and {@link Analyzer}s.
     *
     * @param defaultAnalyzer The default {@link Analyzer} to be used.
     * @param mappers         The per field {@link Mapper}s builders to be used.
     * @param analyzers       The per field {@link Analyzer}s to be used.
     */
    public Schema(Analyzer defaultAnalyzer, Map<String, Mapper> mappers, Map<String, Analyzer> analyzers) {
        this.mappers = mappers;
        this.analyzer = new SchemaAnalyzer(defaultAnalyzer, analyzers, mappers);
        mappedColumns = new HashSet<>();
        for (Mapper mapper : this.mappers.values()) {
            mappedColumns.addAll(mapper.mappedColumns);
        }
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
        Mapper mapper;

        String fieldName = Column.getMapperNameByFullName(field);

        mapper = mappers.get(fieldName);
        if (mapper != null) {
            return mapper;
        }

        String[] components = field.split("\\.");
        for (int i = components.length - 1; i >= 0; i--) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                sb.append(components[j]);
                if (j < i) {
                    sb.append('.');
                }
            }
            mapper = mappers.get(sb.toString());
            if (mapper != null) {
                return mapper;
            }
        }
        return null;
    }

    /**
     * Validates the specified {@link Columns} for mapping.
     *
     * @param columns The {@link Columns} to be validated.
     */
    public void validate(Columns columns) {
        Document document = new Document();
        for (Mapper mapper : mappers.values()) {
            mapper.addFields(document, columns);
        }
    }

    /**
     * Adds to the specified {@link Document} the Lucene fields representing the specified {@link Columns}.
     *
     * This is done in a best-effort way, so each mapper errors are logged and ignored.
     *
     * @param document The Lucene {@link Document} where the fields are going to be added.
     * @param columns  The {@link Columns} to be added.
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

    /**
     * Returns if there is any mapper mapping the specified column.
     *
     * @param column A column name.
     * @return {@code true} if there is any mapper mapping the specified column, {@code false} otherwise.
     */
    public boolean maps(String column) {
        for (String mappedColumn : mappedColumns) {
            String name = mappedColumn.contains(".")
                          ? mappedColumn.substring(0, mappedColumn.indexOf("."))
                          : mappedColumn;
            if (column.equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns if the specified {@link Columns} contains the all the mapped columns.
     *
     * @param columns A {@link Columns}.
     * @return {@code true} if the specified {@link Columns} contains the mapped columns, {@code false} otherwise.
     */
    public boolean mapsAll(Columns columns) {
        for (Mapper mapper : mappers.values()) {
            if (!mapper.maps(columns)) {
                return false;
            }
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        analyzer.close();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("mappers", mappers).add("analyzer", analyzer).toString();
    }
}
