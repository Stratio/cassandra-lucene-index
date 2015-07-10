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
package com.stratio.cassandra.lucene.service;

import org.apache.cassandra.db.Row;
import org.apache.lucene.search.ScoreDoc;

/**
 * Class representing a Cassandra {@link Row} associated to a Lucene {@link ScoreDoc}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ScoredRow {

    private final Row row;
    private final ScoreDoc scoreDoc;

    /**
     * Builds a new scored row.
     *
     * @param row      The Cassandra {@link Row}.
     * @param scoreDoc The Lucene {@link ScoreDoc}.
     */
    public ScoredRow(Row row, ScoreDoc scoreDoc) {
        this.row = row;
        this.scoreDoc = scoreDoc;
    }

    /**
     * Returns the Cassandra {@link Row}.
     *
     * @return The Cassandra {@link Row}.
     */
    public Row getRow() {
        return row;
    }

    /**
     * Returns the Lucene {@link ScoreDoc}.
     *
     * @return The Lucene {@link ScoreDoc}.
     */
    public ScoreDoc getScoreDoc() {
        return scoreDoc;
    }

    /**
     * Returns the numeric score.
     *
     * @return The numeric score.
     */
    public Float getScore() {
        return scoreDoc.score;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScoredRow that = (ScoredRow) o;

        return scoreDoc.doc == that.scoreDoc.doc;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return scoreDoc.doc;
    }
}
