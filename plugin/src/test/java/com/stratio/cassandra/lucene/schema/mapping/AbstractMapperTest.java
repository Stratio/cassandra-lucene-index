package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.schema.mapping.builder.MapperBuilder;
import com.stratio.cassandra.lucene.util.JsonSerializer;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class AbstractMapperTest {

    protected void testJson(MapperBuilder<?> mapperBuilder, String json) {
        try {
            String json1 = JsonSerializer.toString(mapperBuilder);
            assertEquals("JSON serialization is wrong", json, json1);
            String json2 = JsonSerializer.toString(JsonSerializer.fromString(json1, MapperBuilder.class));
            assertEquals("JSON serialization is wrong", json1, json2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
