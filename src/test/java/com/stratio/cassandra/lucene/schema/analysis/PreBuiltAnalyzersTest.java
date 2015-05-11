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
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class PreBuiltAnalyzersTest {

    @Test
    public void testGetStandard() {
        Analyzer analyzer = PreBuiltAnalyzers.STANDARD.get();
        Assert.assertEquals(StandardAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testGetDefault() {
        Analyzer analyzer = PreBuiltAnalyzers.DEFAULT.get();
        Assert.assertEquals(StandardAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testGetKeyword() {
        Analyzer analyzer = PreBuiltAnalyzers.KEYWORD.get();
        Assert.assertEquals(KeywordAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testGetStop() {
        Analyzer analyzer = PreBuiltAnalyzers.STOP.get();
        Assert.assertEquals(StopAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testGetWhitespace() {
        Analyzer analyzer = PreBuiltAnalyzers.WHITESPACE.get();
        Assert.assertEquals(WhitespaceAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testGetSimple() {
        Analyzer analyzer = PreBuiltAnalyzers.SIMPLE.get();
        Assert.assertEquals(SimpleAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testGetClassic() {
        Analyzer analyzer = PreBuiltAnalyzers.CLASSIC.get();
        Assert.assertEquals(ClassicAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testGetArabic() {
        Analyzer analyzer = PreBuiltAnalyzers.ARABIC.get();
        Assert.assertEquals(ArabicAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testGetArmenian() {
        Analyzer analyzer = PreBuiltAnalyzers.ARMENIAN.get();
        Assert.assertEquals(ArmenianAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testGetFromNameLowerCase() {
        Analyzer analyzer = PreBuiltAnalyzers.get("standard");
        Assert.assertEquals(StandardAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testGetFromNameUpperCase() {
        Analyzer analyzer = PreBuiltAnalyzers.get("STANDARD");
        Assert.assertEquals(StandardAnalyzer.class, analyzer.getClass());
    }

}
