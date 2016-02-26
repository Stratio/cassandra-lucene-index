package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoOperationTest extends AbstractConditionTest  {

    @Test(expected = IndexException.class)
    public void testParseNull() {
        GeoOperation.parse(null);
    }

    @Test(expected = IndexException.class)
    public void testParseEmpty() {
        GeoOperation.parse("");
    }

    @Test(expected = IndexException.class)
    public void testParseBlank() {
        GeoOperation.parse("\t ");
    }

    @Test(expected = IndexException.class)
    public void testInvalid() {
        GeoOperation.parse("invalid_operation");
    }

    @Test
    public void testParseIntersects() {
        GeoOperation distance = GeoOperation.parse("intersects");
        check(distance, SpatialOperation.Intersects);
    }

    @Test
    public void testParseIsWithin() {
        GeoOperation distance = GeoOperation.parse("is_within");
        check(distance, SpatialOperation.IsWithin);
    }

    @Test
    public void testParseContains() {
        GeoOperation distance = GeoOperation.parse("contains");
        check(distance, SpatialOperation.Contains);
    }

    private void check(GeoOperation operation, SpatialOperation spatialOperation) {
        assertEquals("Parsed distance is wrong", spatialOperation, operation.getSpatialOperation());
    }
}
