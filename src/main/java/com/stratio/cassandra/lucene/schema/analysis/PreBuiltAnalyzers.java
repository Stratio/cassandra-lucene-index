/*
 * Copyright 2009-2014 Elasticsearch.
 * Modification and adaptations - Copyright 2015, Stratio.
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
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;

import java.util.Locale;

/**
 * Prebuilt Lucene {@link Analyzer}s that can be instantiated by name.
 */
public enum PreBuiltAnalyzers {

    STANDARD() {
        @Override
        protected Analyzer instantiate() {
            return new StandardAnalyzer();
        }
    },

    DEFAULT() {
        @Override
        protected Analyzer instantiate() {
            return STANDARD.get();
        }
    },

    KEYWORD() {
        @Override
        protected Analyzer instantiate() {
            return new KeywordAnalyzer();
        }
    },

    STOP {
        @Override
        protected Analyzer instantiate() {
            return new StopAnalyzer();

        }
    },

    WHITESPACE {
        @Override
        protected Analyzer instantiate() {
            return new WhitespaceAnalyzer();

        }
    },

    SIMPLE {
        @Override
        protected Analyzer instantiate() {
            return new SimpleAnalyzer();

        }
    },

    CLASSIC {
        @Override
        protected Analyzer instantiate() {
            return new ClassicAnalyzer();

        }
    },

    ARABIC {
        @Override
        protected Analyzer instantiate() {
            return new ArabicAnalyzer();
        }
    },

    ARMENIAN {
        @Override
        protected Analyzer instantiate() {
            return new ArmenianAnalyzer();
        }
    },

    BASQUE {
        @Override
        protected Analyzer instantiate() {
            return new BasqueAnalyzer();
        }
    },

    BRAZILIAN {
        @Override
        protected Analyzer instantiate() {
            return new BrazilianAnalyzer();
        }
    },

    BULGARIAN {
        @Override
        protected Analyzer instantiate() {
            return new BulgarianAnalyzer();
        }
    },

    CATALAN {
        @Override
        protected Analyzer instantiate() {
            return new CatalanAnalyzer();
        }
    },

    CHINESE() {
        @Override
        protected Analyzer instantiate() {
            return new StandardAnalyzer();
        }
    },

    CJK {
        @Override
        protected Analyzer instantiate() {
            return new CJKAnalyzer();
        }
    },

    CZECH {
        @Override
        protected Analyzer instantiate() {
            return new CzechAnalyzer();
        }
    },

    DUTCH {
        @Override
        protected Analyzer instantiate() {
            return new DutchAnalyzer();

        }
    },

    DANISH {
        @Override
        protected Analyzer instantiate() {
            return new DanishAnalyzer();

        }
    },

    ENGLISH {
        @Override
        protected Analyzer instantiate() {
            return new EnglishAnalyzer();
        }
    },

    FINNISH {
        @Override
        protected Analyzer instantiate() {
            return new FinnishAnalyzer();
        }
    },

    FRENCH {
        @Override
        protected Analyzer instantiate() {
            return new FrenchAnalyzer();
        }
    },

    GALICIAN {
        @Override
        protected Analyzer instantiate() {
            return new GalicianAnalyzer();
        }
    },

    GERMAN {
        @Override
        protected Analyzer instantiate() {
            return new GermanAnalyzer();
        }
    },

    GREEK {
        @Override
        protected Analyzer instantiate() {
            return new GreekAnalyzer();
        }
    },

    HINDI {
        @Override
        protected Analyzer instantiate() {
            return new HindiAnalyzer();

        }
    },

    HUNGARIAN {
        @Override
        protected Analyzer instantiate() {
            return new HungarianAnalyzer();
        }
    },

    INDONESIAN {
        @Override
        protected Analyzer instantiate() {
            return new IndonesianAnalyzer();
        }
    },

    IRISH {
        @Override
        protected Analyzer instantiate() {
            return new IrishAnalyzer();
        }
    },

    ITALIAN {
        @Override
        protected Analyzer instantiate() {
            return new ItalianAnalyzer();
        }
    },

    LATVIAN {
        @Override
        protected Analyzer instantiate() {
            return new LatvianAnalyzer();
        }
    },

    NORWEGIAN {
        @Override
        protected Analyzer instantiate() {
            return new NorwegianAnalyzer();
        }
    },

    PERSIAN {
        @Override
        protected Analyzer instantiate() {
            return new PersianAnalyzer();
        }
    },

    PORTUGUESE {
        @Override
        protected Analyzer instantiate() {
            return new PortugueseAnalyzer();
        }
    },

    ROMANIAN {
        @Override
        protected Analyzer instantiate() {
            return new RomanianAnalyzer();

        }
    },

    RUSSIAN {
        @Override
        protected Analyzer instantiate() {
            return new RussianAnalyzer();
        }
    },

    SORANI {
        @Override
        protected Analyzer instantiate() {
            return new SoraniAnalyzer();
        }
    },

    SPANISH {
        @Override
        protected Analyzer instantiate() {
            return new SpanishAnalyzer();
        }
    },

    SWEDISH {
        @Override
        protected Analyzer instantiate() {
            return new SwedishAnalyzer();
        }
    },

    TURKISH {
        @Override
        protected Analyzer instantiate() {
            return new TurkishAnalyzer();
        }
    },

    THAI {
        @Override
        protected Analyzer instantiate() {
            return new ThaiAnalyzer();
        }
    };

    private Analyzer analyzer;

    public synchronized Analyzer get() {
        if (analyzer == null) analyzer = instantiate();
        return analyzer;
    }

    abstract protected Analyzer instantiate();

    public static Analyzer get(String name) {
        try {
            return valueOf(name.toUpperCase(Locale.ROOT)).get();
        } catch (IllegalArgumentException ie) {
            return null;
        }
    }
}
