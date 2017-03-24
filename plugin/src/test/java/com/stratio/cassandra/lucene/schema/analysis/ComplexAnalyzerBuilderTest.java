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

import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ComplexAnalyzerBuilderTest {
    @Test
    public void testAdvancedAnaylizerInstantiation() throws IOException {
        final String json = "{" +
                "type:\"complex\"," +
                "tokenizer:{\"class\":\"ngram\", \"parameters\":[\"1\",\"2\"]}," +
                "token_streams:[" +
                "  {" +
                    "\"class\":\"stop\"," +
                    "\"parameters\":[null, \"a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on," +
                                            "or,such,that,the,their,then,there,these,they,this,to,was,will,with\"]" +
                "  }," +
                "  {" +
                    "\"class\":\"org.apache.lucene.analysis.core.LowerCaseFilter\"," +
                    "\"parameters\":[null]" +
                "  }," +
                "  {" +
                    "\"class\":\"org.apache.lucene.analysis.standard.StandardFilter\"," +
                    "\"parameters\":[null]" +
                "  }" +
                "]}";
        final AnalyzerBuilder builder = JsonSerializer.fromString(json, AnalyzerBuilder.class);
        final Analyzer analyzer = builder.analyzer();
        assertNotNull("Expected not null analyzer", analyzer);
        final List<String> tokens = analyze("the dogs xx are hungry yy", analyzer);
        assertEquals(48, tokens.size());
        // test a few but we are already good if we are there
        assertTrue(tokens.containsAll(asList("t", "th", "do", "un")));
        analyzer.close();
    }

    private List<String> analyze(String value, Analyzer analyzer) {
        List<String> result = new ArrayList<>();
        TokenStream stream = null;
        try {
            stream = analyzer.tokenStream(null, value);
            stream.reset();
            while (stream.incrementToken()) {
                String analyzedValue = stream.getAttribute(CharTermAttribute.class).toString();
                result.add(analyzedValue);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeWhileHandlingException(stream);
        }
        return result;
    }
}
