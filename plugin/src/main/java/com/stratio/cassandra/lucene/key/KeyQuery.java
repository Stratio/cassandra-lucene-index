/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link MultiTermQuery} to get a range of clustering keys.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class KeyQuery extends MultiTermQuery {

    public static final Logger logger = LoggerFactory.getLogger(KeyMapper.class);

    private final KeyMapper mapper;
    private final DecoratedKey key;
    private final Token token;
    private final ClusteringPrefix start, stop;
    private final ClusteringComparator clusteringComparator;
    private final boolean acceptLowerConflicts, acceptUpperConflicts;

    /**
     * Returns a new clustering key query for the specified clustering key range using the specified mapper.
     *
     * @param mapper the clustering key mapper to be used
     * @param key the partition key
     * @param start the start clustering
     * @param stop the stop clustering
     * @param acceptLowerConflicts if accept lower token conflicts
     * @param acceptUpperConflicts if accept upper token conflicts
     */
    public KeyQuery(KeyMapper mapper,
                    DecoratedKey key,
                    ClusteringPrefix start,
                    ClusteringPrefix stop,
                    boolean acceptLowerConflicts,
                    boolean acceptUpperConflicts) {
        super(KeyMapper.FIELD_NAME);
        this.mapper = mapper;
        this.key = key;
        this.token = key.getToken();
        this.start = start;
        this.stop = stop;
        this.acceptLowerConflicts = acceptLowerConflicts;
        this.acceptUpperConflicts = acceptUpperConflicts;
        clusteringComparator = mapper.clusteringComparator();
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
                                        .append("key", key)
                                        .append("start", start == null ? null : mapper.toString(start))
                                        .append("stop", stop == null ? null : mapper.toString(stop))
                                        .toString();
    }

    private class FullKeyDataRangeFilteredTermsEnum extends FilteredTermsEnum {

        FullKeyDataRangeFilteredTermsEnum(TermsEnum tenum) {
            super(tenum);
            setInitialSeekTerm(mapper.bytesRef(key, new Clustering(start.getRawValues())));

        }

        /** {@inheritDoc} */
        @Override
        protected AcceptStatus accept(BytesRef term) {

            KeyEntry entry = mapper.entry(term);

            // Check token
            int tokenComparison = entry.getToken().compareTo(token);
            if (tokenComparison < 0) {
                return AcceptStatus.NO;
            }
            if (tokenComparison > 0) {
                return AcceptStatus.END;
            }

            // Check partition key
            Integer keyComparison = entry.getDecoratedKey().compareTo(key);
            if (keyComparison < 0 && !acceptLowerConflicts) {
                return AcceptStatus.NO;
            }
            if (keyComparison > 0 && !acceptUpperConflicts) {
                return AcceptStatus.NO;
            }

            // Check clustering key range
            Clustering clustering = entry.getClustering();
            if (start != null && clusteringComparator.compare(start, clustering) > 0) {
                return AcceptStatus.NO;
            }
            if (stop != null && clusteringComparator.compare(stop, clustering) < 0) {
                return AcceptStatus.NO;
            }

            return AcceptStatus.YES;
        }
    }
}