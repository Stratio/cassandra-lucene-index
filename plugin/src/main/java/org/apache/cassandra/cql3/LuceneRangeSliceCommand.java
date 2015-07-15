package org.apache.cassandra.cql3;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LuceneRangeSliceCommand extends AbstractRangeCommand {

    public static final LuceneRangeSliceCommandSerializer serializer = new LuceneRangeSliceCommandSerializer();

    int maxResults;
    boolean countCQL3Rows;
    boolean isPaging;

    public LuceneRangeSliceCommand(String keyspace,
                                   String columnFamily,
                                   long timestamp,
                                   AbstractBounds<RowPosition> keyRange,
                                   IDiskAtomFilter predicate,
                                   List<IndexExpression> rowFilter,
                                   int maxResults,
                                   boolean countCQL3Rows,
                                   boolean isPaging) {
        super(keyspace, columnFamily, timestamp, keyRange, predicate, rowFilter);
        this.maxResults = maxResults;
        this.countCQL3Rows = countCQL3Rows;
        this.isPaging = isPaging;
    }

    public List<Row> executeLocally() {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);

        ExtendedFilter exFilter = cfs.makeExtendedFilter(keyRange,
                                                         predicate,
                                                         rowFilter,
                                                         maxResults,
                                                         countCQL3Rows,
                                                         false,
                                                         timestamp);
        return cfs.search(exFilter);
    }

    public MessageOut<LuceneRangeSliceCommand> createMessage() {
        return new MessageOut<>(MessagingService.Verb.RANGE_SLICE, this, serializer);
    }

    public AbstractRangeCommand forSubRange(AbstractBounds<RowPosition> subRange)
    {
        return new LuceneRangeSliceCommand(keyspace,
                                     columnFamily,
                                     timestamp,
                                     subRange,
                                     predicate.cloneShallow(),
                                     rowFilter,
                                     maxResults,
                                     countCQL3Rows,
                                     isPaging);
    }

    public AbstractRangeCommand withUpdatedLimit(int newLimit)
    {
        return new LuceneRangeSliceCommand(keyspace,
                                     columnFamily,
                                     timestamp,
                                           keyRange,
                                     predicate.cloneShallow(),
                                     rowFilter,
                                     newLimit,
                                     countCQL3Rows,
                                     isPaging);
    }

    public int limit()
    {
        return maxResults;
    }

    public boolean countCQL3Rows()
    {
        return countCQL3Rows;
    }

}

class LuceneRangeSliceCommandSerializer implements IVersionedSerializer<LuceneRangeSliceCommand> {

    public void serialize(LuceneRangeSliceCommand sliceCommand, DataOutputPlus out, int version) throws IOException {
        out.writeUTF(sliceCommand.keyspace);
        out.writeUTF(sliceCommand.columnFamily);
        out.writeLong(sliceCommand.timestamp);

        CFMetaData metadata = Schema.instance.getCFMetaData(sliceCommand.keyspace, sliceCommand.columnFamily);

        metadata.comparator.diskAtomFilterSerializer().serialize(sliceCommand.predicate, out, version);

        if (sliceCommand.rowFilter == null) {
            out.writeInt(0);
        } else {
            out.writeInt(sliceCommand.rowFilter.size());
            for (IndexExpression expr : sliceCommand.rowFilter) {
                expr.writeTo(out);
            }
        }
        AbstractBounds.serializer.serialize(sliceCommand.keyRange, out, version);
        out.writeInt(sliceCommand.maxResults);
        out.writeBoolean(sliceCommand.countCQL3Rows);
        out.writeBoolean(sliceCommand.isPaging);
    }

    public LuceneRangeSliceCommand deserialize(DataInput in, int version) throws IOException {
        String keyspace = in.readUTF();
        String columnFamily = in.readUTF();
        long timestamp = in.readLong();

        CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);
        if (metadata == null) {
            String message = String.format(
                    "Got range slice command for nonexistent table %s.%s.  If the table was just " +
                    "created, this is likely due to the schema not being fully propagated.  Please wait for schema " +
                    "agreement on table creation.",
                    keyspace,
                    columnFamily);
            throw new UnknownColumnFamilyException(message, null);
        }

        IDiskAtomFilter predicate = metadata.comparator.diskAtomFilterSerializer().deserialize(in, version);

        List<IndexExpression> rowFilter;
        int filterCount = in.readInt();
        rowFilter = new ArrayList<>(filterCount);
        for (int i = 0; i < filterCount; i++) {
            rowFilter.add(IndexExpression.readFrom(in));
        }
        AbstractBounds<RowPosition> range = AbstractBounds.serializer.deserialize(in, version).toRowBounds();

        int maxResults = in.readInt();
        boolean countCQL3Rows = in.readBoolean();
        boolean isPaging = in.readBoolean();
        return new LuceneRangeSliceCommand(keyspace,
                                           columnFamily,
                                           timestamp,
                                           range,
                                           predicate,
                                           rowFilter,
                                           maxResults,
                                           countCQL3Rows,
                                           isPaging);
    }

    public long serializedSize(LuceneRangeSliceCommand rsc, int version) {
        long size = TypeSizes.NATIVE.sizeof(rsc.keyspace);
        size += TypeSizes.NATIVE.sizeof(rsc.columnFamily);
        size += TypeSizes.NATIVE.sizeof(rsc.timestamp);

        CFMetaData metadata = Schema.instance.getCFMetaData(rsc.keyspace, rsc.columnFamily);

        IDiskAtomFilter filter = rsc.predicate;

        size += metadata.comparator.diskAtomFilterSerializer().serializedSize(filter, version);

        if (rsc.rowFilter == null) {
            size += TypeSizes.NATIVE.sizeof(0);
        } else {
            size += TypeSizes.NATIVE.sizeof(rsc.rowFilter.size());
            for (IndexExpression expr : rsc.rowFilter) {
                size += TypeSizes.NATIVE.sizeofWithShortLength(expr.column);
                size += TypeSizes.NATIVE.sizeof(expr.operator.ordinal());
                size += TypeSizes.NATIVE.sizeofWithShortLength(expr.value);
            }
        }
        size += AbstractBounds.serializer.serializedSize(rsc.keyRange, version);
        size += TypeSizes.NATIVE.sizeof(rsc.maxResults);
        size += TypeSizes.NATIVE.sizeof(rsc.countCQL3Rows);
        size += TypeSizes.NATIVE.sizeof(rsc.isPaging);
        return size;
    }
}
