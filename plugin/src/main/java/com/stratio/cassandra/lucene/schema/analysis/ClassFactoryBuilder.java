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
package com.stratio.cassandra.lucene.schema.analysis;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.lucene.analysis.util.CharArraySet;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;

/**
 * Build an instance of a class based on the constructor matching the same number of parameters.
 * The coercion of parameters only supports simple types like String, int, long.
 */
public class ClassFactoryBuilder {
    @JsonProperty("class")
    private final String className;

    @JsonProperty("parameters")
    private final String[] parameters;

    @JsonCreator
    public ClassFactoryBuilder(@JsonProperty("class") String className, @JsonProperty("parameters") String[] parameters) {
        this.className = replaceAlias(className);
        this.parameters = parameters;
    }

    private String replaceAlias(final String className) {
        // support edge-ngram or edge_ngram or edgengram styles depending what is the more readable for the user
        switch (className.replace("_", "").replace("-", "").toLowerCase(ENGLISH)) {
            // tokenizers
            case "ngram":
                return "org.apache.lucene.analysis.ngram.NGramTokenizer";
            case "edgengram":
                return "org.apache.lucene.analysis.ngram.EdgeNGramTokenizer";
            case "pattern":
                return "org.apache.lucene.analysis.pattern.PatternTokenizer";
            case "classic":
                return "org.apache.lucene.analysis.standard.ClassicTokenizer";
            case "keyword":
                return "org.apache.lucene.analysis.core.KeywordTokenizer";
            // filters
            case "limitcount":
                return "org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter";
            case "limitoffset":
                return "org.apache.lucene.analysis.miscellaneous.LimitTokenOffsetFilter";
            case "limitposition":
                return "org.apache.lucene.analysis.miscellaneous.LimitTokenPositionFilter";
            case "edgengramfilter":
                return "org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter";
            case "ngramfilter":
                return "org.apache.lucene.analysis.ngram.NGramTokenFilter";
            case "kstemfilter":
                return "org.apache.lucene.analysis.en.KStemFilter";
            case "shingle":
                return "org.apache.lucene.analysis.shingle.ShingleFilter";
            case "trim":
                return "org.apache.lucene.analysis.miscellaneous.TrimFilter";
            case "stop":
                return "org.apache.lucene.analysis.core.StopFilter";
            case "lower":
                return "org.apache.lucene.analysis.core.LowerCaseFilter";
            case "standard":
                return "org.apache.lucene.analysis.standard.StandardFilter";
            case "numericpayload":
                return "org.apache.lucene.analysis.payloads.NumericPayloadTokenFilter";
            default:
                return className;
        }
    }

    public <T> T build(final Class<T> expected, final Function<Class<?>, Object> valueProvider) {
        try {
            final Class<?> impl = Class.forName(className);
            if (!expected.isAssignableFrom(impl)) {
                throw new IndexException("'%s' doesn't implement '%s'", className, expected.getName());
            }

            if (parameters == null || parameters.length == 0) {
                return expected.cast(impl.getConstructor().newInstance());
            }

            final List<Constructor<?>> collect = Stream.of(impl.getConstructors())
                    .filter(c -> c.getParameterCount() == parameters.length && Modifier.isPublic(c.getModifiers()))
                    .collect(toList());
            if (collect.isEmpty()) {
                throw new IndexException("No constructor with %s parameters in '%s'", parameters.length, className);
            }
            if (collect.size() > 1) {
                throw new IndexException("Ambiguous constructor with %s parameters in '%s'", parameters.length, className);
            }

            final Object[] args = new Object[parameters.length];
            final Constructor<?> next = collect.iterator().next();
            final Class<?>[] types = next.getParameterTypes();
            for (int i = 0; i < args.length; i++) {
                if (types[i] == int.class) {
                    args[i] = Integer.parseInt(parameters[i]);
                } else if (types[i] == long.class) {
                    args[i] = Long.parseLong(parameters[i]);
                } else if (types[i] == short.class) {
                    args[i] = Short.parseShort(parameters[i]);
                } else if (types[i] == boolean.class) {
                    args[i] = Boolean.parseBoolean(parameters[i]);
                } else if (types[i] == float.class) {
                    args[i] = Float.parseFloat(parameters[i]);
                } else if (types[i] == String.class) {
                    args[i] = parameters[i];
                } else if (types[i] == CharArraySet.class) {
                    args[i] = new CharArraySet(asList(parameters[i].split(",")), true);
                } else {
                    if (valueProvider != null) {
                        final Object value = valueProvider.apply(types[i]);
                        if (value != null) {
                            args[i] = value;
                            continue;
                        }
                    }
                    throw new IndexException("Unsupported constructor parameter type '%s' for '%s'", types[i].getName(), className);
                }
            }

            return expected.cast(next.newInstance(args));
        } catch (final Exception e) {
            throw new IndexException(e, "Didn't find '%s'", className);
        }
    }
}
