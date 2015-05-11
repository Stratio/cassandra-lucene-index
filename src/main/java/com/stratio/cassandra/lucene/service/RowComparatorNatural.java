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
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.dht.Token;

import java.util.Comparator;

/**
 * A {@link Comparator} for comparing {@link Row}s according to its Cassandra's natural order.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowComparatorNatural implements RowComparator {

    private final ComparatorChain<Row> comparatorChain;

    /**
     * Builds a new {@link RowComparatorNatural} to be used with skinny rows tables.
     */
    public RowComparatorNatural() {
        super();
        comparatorChain = new ComparatorChain<>();
        comparatorChain.addComparator(new Comparator<Row>() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public int compare(Row row1, Row row2) {
                Token t1 = row1.key.getToken();
                Token t2 = row2.key.getToken();
                return t1.compareTo(t2);
            }
        });
    }

    /**
     * Builds a new {@link RowComparatorNatural} to be used with wide rows tables.
     *
     * @param clusteringKeyMapper The used {@link ClusteringKeyMapper}.
     */
    public RowComparatorNatural(final ClusteringKeyMapper clusteringKeyMapper) {
        super();
        comparatorChain = new ComparatorChain<>();
        comparatorChain.addComparator(new Comparator<Row>() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public int compare(Row row1, Row row2) {
                Token t1 = row1.key.getToken();
                Token t2 = row2.key.getToken();
                return t1.compareTo(t2);
            }
        });
        comparatorChain.addComparator(new Comparator<Row>() {
            @Override
            public int compare(Row row1, Row row2) {
                CellNameType nameType = clusteringKeyMapper.getType();
                CellName name1 = clusteringKeyMapper.clusteringKey(row1);
                CellName name2 = clusteringKeyMapper.clusteringKey(row2);
                return nameType.compare(name1, name2);
            }
        });
    }

    @Override
    public int compare(Row row1, Row row2) {
        return comparatorChain.compare(row1, row2);
    }

}
