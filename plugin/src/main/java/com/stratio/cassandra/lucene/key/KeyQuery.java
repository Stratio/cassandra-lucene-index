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

import com.google.common.base.Objects;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@link MultiTermQuery} to get a range of clustering keys.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class KeyQuery extends MultiTermQuery {
    private static final Logger logger = LoggerFactory.getLogger(KeyQuery.class);
    private final KeyMapper mapper;
    private final DecoratedKey key;
    private final ByteBuffer collatedToken;
    private final Composite start, stop;
    private final CellNameType clusteringComparator;
    private final BytesRef seek;

    /**
     * Returns a new clustering key query for the specified clustering key range using the specified mapper.
     *
     * @param mapper the clustering key mapper to be used
     * @param key the partition key
     * @param start the start clustering
     * @param stop the stop clustering
     */
    KeyQuery(KeyMapper mapper,
             DecoratedKey key,
             Composite start,
             Composite stop) {
        super(KeyMapper.FIELD_NAME);
        this.mapper = mapper;
        this.key = key;
        this.collatedToken = TokenMapper.toCollated(key.getToken());
        this.start = start;
        this.stop = stop;
        clusteringComparator = mapper.clusteringComparator();
        seek = mapper.seek(key);
    }

    /** {@inheritDoc} */
    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        return new FullKeyDataRangeFilteredTermsEnum(terms.iterator());
    }

    /** {@inheritDoc} */
    @Override
    public String toString(String field) {
        return Objects.toStringHelper(this)
                      .add("field", field)
                      .add("key", key)
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

        KeyQuery keyQuery = (KeyQuery) o;

        if (!key.equals(keyQuery.key)) {
            return false;
        }
        if (start != null ? !start.equals(keyQuery.start) : keyQuery.start != null) {
            return false;
        }
        return stop != null ? stop.equals(keyQuery.stop) : keyQuery.stop == null;
    }

    /**
     * {@inheritDoc}
     *
     * Important to avoid collisions in Lucene's query cache.
     */
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + (start != null ? start.hashCode() : 0);
        result = 31 * result + (stop != null ? stop.hashCode() : 0);
        return result;
    }

    private class FullKeyDataRangeFilteredTermsEnum extends FilteredTermsEnum {

        FullKeyDataRangeFilteredTermsEnum(TermsEnum tenum) {
            super(tenum);
            setInitialSeekTerm(seek);
        }

        /** {@inheritDoc} */
        @Override
        protected AcceptStatus accept(BytesRef term) {

            KeyEntry entry = mapper.entry(term);

            // Check token
            int tokenComparison = UTF8Type.instance.compare(entry.getCollatedToken(), collatedToken);
            if (tokenComparison < 0) {
                return AcceptStatus.NO;
            }
            if (tokenComparison > 0) {
                return AcceptStatus.END;
            }

            // Check partition key
            Integer keyComparison = entry.getDecoratedKey().compareTo(key);
            if (keyComparison < 0) {
                return AcceptStatus.NO;
            }
            if (keyComparison > 0) {
                return AcceptStatus.NO;
            }

            // Check clustering key range
            Composite clustering = entry.getComposite();
            if (start != null && !start.isEmpty() && clusteringComparator.compare(start, clustering) > 0) {
                return AcceptStatus.NO;
            }
            if (stop != null && !stop.isEmpty() && clusteringComparator.compare(stop, clustering) < 0) {
                return AcceptStatus.NO;
            }

            return AcceptStatus.YES;
        }
    }
}