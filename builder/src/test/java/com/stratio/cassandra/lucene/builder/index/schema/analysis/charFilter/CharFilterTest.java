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
package com.stratio.cassandra.lucene.builder.index.schema.analysis.charFilter;

import org.junit.Test;
import java.util.ArrayList;
import static org.junit.Assert.*;


/**
 * Created by jpgilaberte on 14/06/17.
 */
public class CharFilterTest {

    @Test
    public void testHtmlStripCharFilter() {
        ArrayList<String> listTags = new ArrayList<>();
        listTags.add("<br>");
        listTags.add("<script>");
        String classExcepted = "HtmlStripCharFilter";
        String jsonExcepted = "{\"type\":\"htmlstrip\",\"escapedtags\":[\"<br>\",\"<script>\"]}";
        Object testObject = new HtmlStripCharFilter().setEscapedtags(listTags);
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }

    @Test
    public void testMappingCharFilter() {
        String classExcepted = "MappingCharFilter";
        String jsonExcepted = "{\"type\":\"mapping\",\"mapping\":\"path\"}";
        Object testObject = new MappingCharFilter("path");
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }

    @Test
    public void PatternCharFilter() {
        String classExcepted = "PatternCharFilter";
        String jsonExcepted = "{\"type\":\"pattern\",\"pattern\":\"pattern\",\"replacement\":\"replacement\"}";
        Object testObject = new PatternCharFilter("pattern","replacement");
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }

    @Test
    public void testPersianCharFilter() {
        String classExcepted = "PersianCharFilter";
        String jsonExcepted = "{\"type\":\"persian\"}";
        Object testObject = new PersianCharFilter();
        assertTrue(testObject.getClass().getSimpleName().equals(classExcepted));
        assertTrue(testObject.toString().equals(jsonExcepted));
    }
}
