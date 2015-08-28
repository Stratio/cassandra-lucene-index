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

package com.stratio.cassandra.lucene.service;

import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * {@link MultiTermQuery} to get a range of clustering keys.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class ClusteringKeyQuery extends MultiTermQuery {

    private final ClusteringKeyMapper mapper;
    private final Composite start;
    private final Composite stop;
    private final CellNameType type;

    /**
     * Returns a new clustering key query for the specified clustering key range using the specified mapper.
     *
     * @param start  The clustering key at the start of the range.
     * @param stop   The clustering key at the end of the range.
     * @param mapper The clustering key mapper to be used.
     */
    public ClusteringKeyQuery(Composite start, Composite stop, ClusteringKeyMapper mapper) {
        super(ClusteringKeyMapper.FIELD_NAME);
        this.start = start;
        this.stop = stop;
        this.mapper = mapper;
        this.type = mapper.getType();
    }

    /** {@inheritDoc} */
    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        return new FullKeyDataRangeFilteredTermsEnum(terms.iterator());
    }

    /** {@inheritDoc} */
    @Override
    public String toString(String field) {
        return new ToStringBuilder(this).append("field", field)
                                        .append("start", start == null ? null : mapper.toString(start))
                                        .append("stop", stop == null ? null : mapper.toString(stop))
                                        .toString();
    }

    private class FullKeyDataRangeFilteredTermsEnum extends FilteredTermsEnum {

        FullKeyDataRangeFilteredTermsEnum(TermsEnum tenum) {
            super(tenum);
            setInitialSeekTerm(new BytesRef());
        }

        /** {@inheritDoc} */
        @Override
        protected AcceptStatus accept(BytesRef term) {
            CellName clusteringKey = mapper.clusteringKey(term);
            if (start != null && !start.isEmpty() && type.compare(start, clusteringKey) > 0) {
                return AcceptStatus.NO;
            }
            if (stop != null && !stop.isEmpty() && type.compare(stop, clusteringKey) < 0) {
                return AcceptStatus.NO;
            }
            return AcceptStatus.YES;
        }
    }
}
