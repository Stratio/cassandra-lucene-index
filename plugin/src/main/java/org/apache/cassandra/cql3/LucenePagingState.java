package org.apache.cassandra.cql3;

import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LucenePagingState extends PagingState {

    List<? extends AbstractBounds<RowPosition>> ranges;

    public LucenePagingState(List<? extends AbstractBounds<RowPosition>> ranges, int remaining) {
        super(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, remaining);
    }
}
