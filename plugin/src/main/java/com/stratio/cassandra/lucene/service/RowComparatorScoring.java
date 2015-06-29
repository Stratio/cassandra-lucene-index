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
package com.stratio.cassandra.lucene.service;

import org.apache.cassandra.db.Row;

/**
 * A {@link RowComparator} for comparing {@link Row}s according to its Lucene scoring.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowComparatorScoring implements RowComparator {

    /**
     * The used {@link RowService}.
     */
    private final RowService rowService;

    /**
     * Returns a new {@link RowComparator} for comparing {@link Row}s according to its Lucene scoring.
     *
     * @param rowService The used {@link RowService}.
     */
    public RowComparatorScoring(RowService rowService) {
        this.rowService = rowService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compare(Row row1, Row row2) {
        Float score1 = rowService.score(row1);
        Float score2 = rowService.score(row2);
        return score2.compareTo(score1);
    }

}
