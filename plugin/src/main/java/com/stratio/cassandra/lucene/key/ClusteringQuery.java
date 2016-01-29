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

package com.stratio.cassandra.lucene.key;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
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
class ClusteringQuery extends MultiTermQuery {

    private final ClusteringMapper mapper;
    private final ClusteringPrefix start;
    private final ClusteringPrefix stop;
    private final ClusteringComparator comparator;

    /**
     * Returns a new clustering key query for the specified clustering key range using the specified mapper.
     *
     * @param start  the clustering key prefix at the start of the range
     * @param stop   the clustering key prefix at the end of the range
     * @param mapper the clustering key mapper to be used
     */
    public ClusteringQuery(ClusteringPrefix start, ClusteringPrefix stop, ClusteringMapper mapper) {
        super(ClusteringMapper.FIELD_NAME);
        this.start = start;
        this.stop = stop;
        this.mapper = mapper;
        this.comparator = mapper.getComparator();
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
            Clustering clustering = mapper.clustering(term);
            if (start != null && comparator.compare(start, clustering) > 0) {
                return AcceptStatus.NO;
            }
            if (stop != null && comparator.compare(stop, clustering) < 0) {
                return AcceptStatus.NO;
            }
            return AcceptStatus.YES;
        }
    }
}