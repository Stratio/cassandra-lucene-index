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
package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import org.apache.cassandra.db.marshal.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link ColumnMapper} to map a string, tokenized field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperText extends ColumnMapperSingle<String> {

    public static final String DEFAULT_ANALYZER = PreBuiltAnalyzers.DEFAULT.name();

    /** The Lucene {@link Analyzer} to be used. */
    private final String analyzer;

    /**
     * Builds a new {@link ColumnMapperText} using the specified Lucene {@link Analyzer}.
     *
     * @param indexed  If the field supports searching.
     * @param sorted   If the field supports sorting.
     * @param analyzer The Lucene {@link Analyzer} to be used.
     */
    @JsonCreator
    public ColumnMapperText(@JsonProperty("indexed") Boolean indexed,
                            @JsonProperty("sorted") Boolean sorted,
                            @JsonProperty("analyzer") String analyzer) {
        super(indexed,
              sorted,
              AsciiType.instance,
              UTF8Type.instance,
              Int32Type.instance,
              LongType.instance,
              IntegerType.instance,
              FloatType.instance,
              DoubleType.instance,
              BooleanType.instance,
              UUIDType.instance,
              TimeUUIDType.instance,
              TimestampType.instance,
              BytesType.instance,
              InetAddressType.instance);
        this.analyzer = analyzer == null ? DEFAULT_ANALYZER : analyzer;
    }

    /** {@inheritDoc} */
    @Override
    public String getAnalyzer() {
        return analyzer;
    }

    /** {@inheritDoc} */
    @Override
    public String base(String name, Object value) {
        if (value == null) {
            return null;
        } else {
            return value.toString();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Field indexedField(String name, String value) {
        return new TextField(name, value, STORE);
    }

    /** {@inheritDoc} */
    @Override
    public Field sortedField(String name, String value, boolean isCollection) {
        BytesRef bytes = new BytesRef(value);
        if (isCollection) {
            return new SortedSetDocValuesField(name, bytes);
        } else {
            return new SortedDocValuesField(name, bytes);
        }
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(boolean reverse) {
        return new SortField(name, Type.STRING, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public Class<String> baseClass() {
        return String.class;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("indexed", indexed)
                      .add("sorted", sorted)
                      .add("analyzer", analyzer)
                      .toString();
    }
}
