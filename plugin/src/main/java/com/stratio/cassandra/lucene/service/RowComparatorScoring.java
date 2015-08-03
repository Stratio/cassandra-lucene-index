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

import com.stratio.cassandra.lucene.contrib.ComparatorChain;
import com.stratio.cassandra.lucene.search.sort.Sort;
import org.apache.cassandra.db.Row;

import java.util.Comparator;

/**
 * A {@link RowComparator} for comparing {@link Row}s according to its Lucene scoring.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RowComparatorScoring implements RowComparator {

    private final ComparatorChain<Row> comparator;

    /**
     * Returns a new {@link RowComparator} for comparing {@link Row}s according to its Lucene scoring.
     *
     * @param mapper The used {@link RowMapper}.
     */
    public RowComparatorScoring(final RowMapper mapper) {
        comparator = new ComparatorChain<>();
        comparator.addComparator(new Comparator<Row>() {
            @Override
            public int compare(Row row1, Row row2) {
                Float score1 = mapper.score(row1);
                Float score2 = mapper.score(row2);
                return score2.compareTo(score1);
            }
        });
        comparator.addComparator(mapper.comparator());
    }

    /**
     * {@inheritDoc}
     *
     * @param row1 A {@link Row}.
     * @param row2 A {@link Row}.
     * @return A negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater
     * than the second according to a Lucene {@link Sort}.
     */
    @Override
    public int compare(Row row1, Row row2) {
        return comparator.compare(row1, row2);
    }

}
