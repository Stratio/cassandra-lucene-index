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
package com.stratio.cassandra.lucene.schema.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class AnalysisUtils {

    public static List<String> analyzeAsTokens(String value, Analyzer analyzer) {
        return analyzeAsTokens(null, value, analyzer);
    }

    public static List<String> analyzeAsTokens(String field, String value, Analyzer analyzer) {
        List<String> result = new ArrayList<>();
        TokenStream stream = null;
        try {
            stream = analyzer.tokenStream(field, new StringReader(value));
            stream.reset();
            while (stream.incrementToken()) {
                result.add(stream.getAttribute(CharTermAttribute.class).toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeWhileHandlingException(stream);
        }
        return result;
    }

    public static String analyzeAsText(String value, Analyzer analyzer) {
        return analyzeAsText(null, value, analyzer);
    }

    public static String analyzeAsText(String field, String value, Analyzer analyzer) {
        List<String> tokens = AnalysisUtils.analyzeAsTokens(field, value, analyzer);
        StringBuilder result = new StringBuilder();
        for (String token : tokens) {
            result.append(token);
            result.append(" ");
        }
        return result.toString().trim();
    }
}
