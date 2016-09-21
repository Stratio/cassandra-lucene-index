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
package com.stratio.cassandra.lucene.key;

import com.google.common.base.MoreObjects;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.stratio.cassandra.lucene.key.TokenMapper.COLLATION_BYTES;
import static org.apache.cassandra.utils.FastByteOperations.compareUnsigned;

/**
 * {@link MultiTermQuery} to get a range of clustering keys.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ClusteringQuery extends MultiTermQuery {

    private final ClusteringMapper mapper;
    private final Token token;
    private final ClusteringPrefix start, stop;
    private final byte[] seek;

    /**
     * Returns a new clustering key query for the specified clustering key range using the specified mapper.
     *
     * @param mapper the clustering key mapper to be used
     * @param position the partition position
     * @param start the start clustering
     * @param stop the stop clustering
     */
    ClusteringQuery(ClusteringMapper mapper,
                    PartitionPosition position,
                    ClusteringPrefix start,
                    ClusteringPrefix stop) {
        super(ClusteringMapper.FIELD_NAME);
        this.mapper = mapper;
        this.token = position.getToken();
        this.start = start;
        this.stop = stop;
        seek = TokenMapper.collate(token);
    }

    /** {@inheritDoc} */
    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        return new FullKeyDataRangeFilteredTermsEnum(terms.iterator());
    }

    /** {@inheritDoc} */
    @Override
    public String toString(String field) {
        return MoreObjects.toStringHelper(this)
                          .add("field", field)
                          .add("token", token)
                          .add("start", start == null ? null : mapper.toString(start))
                          .add("stop", stop == null ? null : mapper.toString(stop))
                          .toString();
    }

    /**
     * {@inheritDoc}
     *
     * Important to avoid collisions in Lucene's query cache.
     */
    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ClusteringQuery clusteringQuery = (ClusteringQuery) o;

        if (!token.equals(clusteringQuery.token)) {
            return false;
        }
        if (start != null ? !start.equals(clusteringQuery.start) : clusteringQuery.start != null) {
            return false;
        }
        return stop != null ? stop.equals(clusteringQuery.stop) : clusteringQuery.stop == null;
    }

    /**
     * {@inheritDoc}
     *
     * Important to avoid collisions in Lucene's query cache.
     */
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + token.hashCode();
        result = 31 * result + (start != null ? start.hashCode() : 0);
        result = 31 * result + (stop != null ? stop.hashCode() : 0);
        return result;
    }

    private class FullKeyDataRangeFilteredTermsEnum extends FilteredTermsEnum {

        FullKeyDataRangeFilteredTermsEnum(TermsEnum tenum) {
            super(tenum);
            setInitialSeekTerm(new BytesRef(seek));
        }

        /** {@inheritDoc} */
        @Override
        protected AcceptStatus accept(BytesRef term) {

            // Check token range
            int comp = compareUnsigned(term.bytes, 0, COLLATION_BYTES, seek, 0, COLLATION_BYTES);
            if (comp < 0) {
                return AcceptStatus.NO;
            }
            if (comp > 0) {
                return AcceptStatus.END;
            }

            // Check clustering range
            ByteBuffer bb = ByteBuffer.wrap(term.bytes, COLLATION_BYTES, term.length - COLLATION_BYTES);
            Clustering clustering = mapper.clustering(bb);
            if (start != null && mapper.comparator.compare(start, clustering) > 0) {
                return AcceptStatus.NO;
            }
            if (stop != null && mapper.comparator.compare(stop, clustering) < 0) {
                return AcceptStatus.NO;
            }

            return AcceptStatus.YES;
        }
    }
}