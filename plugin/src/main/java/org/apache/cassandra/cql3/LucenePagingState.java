package org.apache.cassandra.cql3;

import com.stratio.cassandra.lucene.IndexSearcher;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.service.RangeHandler;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LucenePagingState {

    private final LastPositions lastPositions;
    private final int remaining;

    public LucenePagingState(PagingState state) throws Exception {
        if (ByteBufferUtils.isEmpty(state.partitionKey)) {
            lastPositions = null;
        } else {
            lastPositions = LastPositions.fromByteBuffer(state.partitionKey);
        }
        remaining = state.remaining;
    }

//    private LastPosition lastPosition(AbstractBounds<RowPosition> range) {
//        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
//        for (LastPosition lastPosition : lastPositions.lastPositions) {
//            DecoratedKey dk = partitioner.decorateKey(lastPosition.partitionKey);
//            if (range.contains(dk))
//        }
//    }

    public PagingState pagingState() throws Exception {
        ByteBuffer serializedLastPositions = lastPositions.toByteBuffer();
        return new PagingState(serializedLastPositions, null, remaining);
    }

    public static class LastPositions implements Serializable {

        LastPosition[] lastPositions;

        public ByteBuffer toByteBuffer() {
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(lastPositions);
                oos.flush();
                return ByteBuffer.wrap(bos.toByteArray());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static LastPositions fromByteBuffer(ByteBuffer bb) {
            try {
                byte[] bytes = ByteBufferUtils.asArray(bb);
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                return (LastPositions) ois.readObject();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class LastPosition implements Serializable {
        byte[] partitionKey;
        byte[] clusteringKey;
    }
}
