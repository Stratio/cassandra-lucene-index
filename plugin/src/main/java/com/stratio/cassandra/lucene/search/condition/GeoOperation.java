package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * Enum representing a spatial operation.
 */
public enum GeoOperation {

    INTERSECTS("intersects", SpatialOperation.Intersects),
    IS_WITHIN("is_within", SpatialOperation.IsWithin),
    IS_DISJOINT_TO("is_disjoint_to", SpatialOperation.IsDisjointTo), // TODO: Not supported by recursive prefix tree strategy
    CONTAINS("contains", SpatialOperation.Contains);

    private final String name;
    private final SpatialOperation spatialOperation;

    GeoOperation(String name, SpatialOperation spatialOperation) {
        this.name = name;
        this.spatialOperation = spatialOperation;
    }

    public SpatialOperation getSpatialOperation() {
        return spatialOperation;
    }

    @JsonCreator
    public static GeoOperation parse(String value) {
        for (GeoOperation geoOperation : values()) {
            String name = geoOperation.name;
            if (name.equalsIgnoreCase(value)) {
                return geoOperation;
            }
        }
        throw new IndexException("Invalid geographic operation %s", value);
    }
}
