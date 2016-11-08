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
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Token;
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
import java.util.Optional;

import static org.apache.cassandra.db.ClusteringPrefix.Kind.*;

/**
 * {@link MultiTermQuery} to get a range of clustering keys.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ClusteringQuery extends MultiTermQuery {

    public static final Logger logger = LoggerFactory.getLogger(ClusteringQuery.class);
    private final ClusteringMapper mapper;

    private final Token token;
    private final ClusteringPrefix start, stop;
    private final ByteBuffer startBB,stopBB;
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

        this.startBB= isStartBounded()?mapper.byteBuffer(token,start):null;
        this.stopBB = isEndBounded()?mapper.byteBuffer(token, stop):null;
        this.seek=isStartBounded()? mapper.byteBuffer(token, start).array():null;

        logger.debug("Building clusteringQuery: {} with position: {}, start: {}, stop: {}",this,position,start,stop);
        logger.debug("Building clusteringQuery: isStartBounded {},isEndBounded {}  includeStartBound {} includeStopBound {}",
                     isStartBounded()?"true":"false",isEndBounded()?"true":"false", includeStartBound()?"true":"false", includeEndBound()?"true":"false");
    }

    private boolean isStartBounded() {
        return (this.start!=null) && (this.start.size()>0);
    }
    private boolean isEndBounded() {
        return (this.stop!=null) && (this.stop.size()>0);
    }

    private boolean includeStartBound() {
        if (!isStartBounded()) return false;
        return (start.kind()==INCL_START_BOUND) || (start.kind()==EXCL_END_INCL_START_BOUNDARY);
    }
    private boolean includeEndBound() {
        if (!isEndBounded()) return false;
        return (stop.kind()==INCL_END_BOUND) || (stop.kind()==INCL_END_EXCL_START_BOUNDARY);

    }
    /** {@inheritDoc} */
    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        return new FullKeyDataRangeFilteredTermsEnum(terms.iterator());
    }

    /** {@inheritDoc} */
    @Override
    public String toString(String field) {
        if (field.isEmpty()) field=ClusteringMapper.FIELD_NAME;
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

        private boolean first;
        FullKeyDataRangeFilteredTermsEnum(TermsEnum tenum) {
            super(tenum);
            setInitialSeekTerm((seek==null)?null:new BytesRef(seek));
            this.first=true;
            logger.debug("Builded FullKeyDataRangeFilteredTermsEnum");
        }

        /** {@inheritDoc} */
        @Override
        protected AcceptStatus accept(BytesRef term) {

            logger.debug("clusteringquery calling acept with term: {}",term);

            ByteBuffer bb1= ByteBufferUtils.byteBuffer(term);
            if ((first) && (startBB!=null)) {
                first=false;
                logger.debug("isFirst and isStartBounded");
                int comp= mapper.compare(bb1,startBB);
                logger.debug("comparing term: {} with startBB: {} resulting {} ",term, startBB, Integer.toString(comp));
                if ((comp==0) && includeStartBound()) {
                    return AcceptStatus.YES;
                }
            }

            if (stopBB!=null) {
                int comp = mapper.compare(bb1, stopBB);
                logger.debug("comparing term: {} with stopBB: {} resulting {} ",term, stopBB, Integer.toString(comp));
                if (comp < 0) {
                    return AcceptStatus.YES;
                } else if (comp == 0) {
                    return includeEndBound() ? AcceptStatus.YES : AcceptStatus.END;
                } else {
                    return AcceptStatus.END;
                }
            }
            return AcceptStatus.YES;
        }
    }
}