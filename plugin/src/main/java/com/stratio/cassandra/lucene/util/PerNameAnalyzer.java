/*
 * Copyright 2015, Stratio.
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

package com.stratio.cassandra.lucene.util;

import com.google.common.base.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Variation of {@link org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper} to be used with CQL.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PerNameAnalyzer extends DelegatingAnalyzerWrapper {

    private final Analyzer defaultAnalyzer;
    private final Map<String, Analyzer> fieldAnalyzers;

    /**
     * Constructs with default analyzer and a map of analyzers to use for specific fields.
     *
     * @param defaultAnalyzer The default {@link Analyzer}s.
     * @param fieldAnalyzers  The per name {@link Analyzer}s.
     */
    public PerNameAnalyzer(Analyzer defaultAnalyzer, Map<String, Analyzer> fieldAnalyzers) {
        super(PER_FIELD_REUSE_STRATEGY);
        this.defaultAnalyzer = new TokenLengthAnalyzer(defaultAnalyzer);
        this.fieldAnalyzers = new HashMap<>(fieldAnalyzers.size());
        for (Map.Entry<String, Analyzer> entry : fieldAnalyzers.entrySet()) {
            this.fieldAnalyzers.put(entry.getKey(), new TokenLengthAnalyzer(entry.getValue()));
        }
    }

    /** {@inheritDoc} */
    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        Analyzer analyzer = fieldAnalyzers.get(fieldName);
        if (analyzer != null) {
            return analyzer;
        } else {
            for (Map.Entry<String, Analyzer> entry : fieldAnalyzers.entrySet()) {
                if (fieldName.startsWith(entry.getKey() + ".")) {
                    return entry.getValue();
                }
            }
            return defaultAnalyzer;
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("defaultAnalyzer", defaultAnalyzer)
                      .add("fieldAnalyzers", fieldAnalyzers)
                      .toString();
    }
}
