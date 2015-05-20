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
package com.stratio.cassandra.lucene.schema;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.analysis.AnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.analysis.ClasspathAnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for several columns mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Schema implements Closeable {

    private final Map<String, ColumnMapper> columnMappers;

    private final Map<String, Analyzer> analyzers;

    private final Analyzer defaultAnalyzer;

    private final Analyzer analyzer;

    /**
     * Builds a new {@code ColumnsMapper} for the specified getAnalyzer and cell mappers.
     *
     * @param columnMappers   The {@link Column} mappers to be used.
     * @param analyzers       The {@link AnalyzerBuilder}s to be used.
     * @param defaultAnalyzer The name of the class of the getAnalyzer to be used.
     */
    @JsonCreator
    public Schema(@JsonProperty("fields") Map<String, ColumnMapper> columnMappers,
                  @JsonProperty("analyzers") Map<String, AnalyzerBuilder> analyzers,
                  @JsonProperty("default_analyzer") String defaultAnalyzer) {

        this.columnMappers = columnMappers;

        this.analyzers = new HashMap<>();
        if (analyzers != null) {
            for (Map.Entry<String, AnalyzerBuilder> entry : analyzers.entrySet()) {
                String name = entry.getKey();
                Analyzer analyzer = entry.getValue().analyzer();
                this.analyzers.put(name, analyzer);
            }
        }

        this.defaultAnalyzer = defaultAnalyzer == null ? PreBuiltAnalyzers.DEFAULT.get() : getAnalyzer(defaultAnalyzer);

        Map<String, Analyzer> perFieldAnalyzers = new HashMap<>();
        for (Map.Entry<String, ColumnMapper> entry : columnMappers.entrySet()) {
            String name = entry.getKey();
            ColumnMapper mapper = entry.getValue();
            String analyzerName = mapper.getAnalyzer();
            Analyzer analyzer = getAnalyzer(analyzerName);
            perFieldAnalyzers.put(name, analyzer);
        }
        this.analyzer = new PerFieldAnalyzerWrapper(this.defaultAnalyzer, perFieldAnalyzers);
    }

    public Analyzer getDefaultAnalyzer() {
        return defaultAnalyzer;
    }

    /**
     * Returns the {@link Analyzer} identified by the specified name. If there is no analyzer with the specified name,
     * then it will be interpreted as a class name and it will be instantiated by reflection.
     * <p/>
     * {@link IllegalArgumentException} is thrown if there is no {@link Analyzer} with such name.
     *
     * @param name The name of the {@link Analyzer} to be returned.
     * @return The {@link Analyzer} identified by the specified name.
     */
    public Analyzer getAnalyzer(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Not null nor empty analyzer name required");
        }
        Analyzer analyzer = analyzers.get(name);
        if (analyzer == null) {
            analyzer = PreBuiltAnalyzers.get(name);
            if (analyzer == null) {
                try {
                    analyzer = (new ClasspathAnalyzerBuilder(name)).analyzer();
                } catch (Exception e) {
                    throw new IllegalArgumentException("Not found analyzer: " + name);
                }
            }
            analyzers.put(name, analyzer);
        }
        return analyzer;
    }

    /**
     * Returns the used {@link Analyzer} wrapper.
     *
     * @return The used {@link Analyzer} wrapper.
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * Returns the {@link ColumnMapper} identified by the specified field name, or {@code null} if not found.
     *
     * @param field A field name.
     * @return The {@link ColumnMapper} identified by the specified field name, or {@code null} if not found.
     */
    public ColumnMapper getMapper(String field) {
        String[] components = field.split("\\.");
        for (int i = components.length - 1; i >= 0; i--) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                sb.append(components[j]);
                if (j < i) sb.append('.');
            }
            ColumnMapper columnMapper = columnMappers.get(sb.toString());
            if (columnMapper != null) return columnMapper;
        }
        return null;
    }

    /**
     * Adds to the specified {@link Document} the Lucene fields representing the specified {@link Columns}.
     *
     * @param document The Lucene {@link Document} where the fields are going to be added.
     * @param columns  The {@link Columns} to be added.
     */
    public void addFields(Document document, Columns columns) {
        for (Column column : columns) {
            String name = column.getName();
            ColumnMapper columnMapper = getMapper(name);
            if (columnMapper != null) {
                columnMapper.addFields(document, column);
            }
        }
    }

    /**
     * Checks if this is consistent with the specified column family metadata.
     *
     * @param metadata A column family metadata.
     */
    public void validate(CFMetaData metadata) {
        for (Map.Entry<String, ColumnMapper> entry : columnMappers.entrySet()) {

            String name = entry.getKey();
            ColumnMapper columnMapper = entry.getValue();
            ByteBuffer columnName = UTF8Type.instance.decompose(name);

            ColumnDefinition columnDefinition = metadata.getColumnDefinition(columnName);
            if (columnDefinition == null) {
                throw new RuntimeException("No column definition for mapper " + name);
            }

            if (columnDefinition.isStatic()) {
                throw new RuntimeException("Lucene indexes are not allowed on static columns as " + name);
            }

            AbstractType<?> type = columnDefinition.type;
            if (!columnMapper.supports(columnDefinition.type)) {
                throw new RuntimeException(String.format("Type '%s' is not supported by mapper '%s'", type, name));
            }
        }
    }

    /**
     * Returns the {@link Schema} contained in the specified JSON {@code String}.
     *
     * @param json A {@code String} containing the JSON representation of the {@link Schema} to be parsed.
     * @return The {@link Schema} contained in the specified JSON {@code String}.
     */
    public static Schema fromJson(String json) throws IOException {
        return JsonSerializer.fromString(json, Schema.class);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        analyzer.close();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("columnMappers", columnMappers)
                      .add("analyzers", analyzers)
                      .add("defaultAnalyzer", defaultAnalyzer)
                      .add("analyzer", analyzer)
                      .toString();
    }
}
