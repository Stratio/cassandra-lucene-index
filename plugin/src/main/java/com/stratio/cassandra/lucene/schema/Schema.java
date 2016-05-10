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
package com.stratio.cassandra.lucene.schema;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
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
     * @param defaultAnalyzer the default {@link Analyzer} to be used
     * @param mappers the per field {@link Mapper}s builders to be used
     * @param analyzers the per field {@link Analyzer}s to be used
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

    public SingleColumnMapper getSingleColumnMapper(String field) {
        Mapper mapper = getMapper(field);
        return mapper == null ? null : (SingleColumnMapper) mapper;
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
