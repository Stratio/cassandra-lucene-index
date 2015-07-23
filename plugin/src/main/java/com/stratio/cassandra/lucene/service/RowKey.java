package com.stratio.cassandra.lucene.service;

import com.google.common.base.Objects;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RowKey {

    private final DecoratedKey partitionKey;
    private final CellName clusteringKey;

    public RowKey(DecoratedKey partitionKey, CellName clusteringKey) {
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
    }

    public DecoratedKey getPartitionKey() {
        return partitionKey;
    }

    public CellName getClusteringKey() {
        return clusteringKey;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("partitionKey", partitionKey)
                      .add("clusteringKey", clusteringKey)
                      .toString();
    }
}
