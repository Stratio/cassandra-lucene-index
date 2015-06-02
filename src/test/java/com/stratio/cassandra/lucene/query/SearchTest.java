package com.stratio.cassandra.lucene.query;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import org.junit.Test;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SearchTest {

    @Test
    public void testBuilderEmpty() {
        Search search = new Search(null, null, null);
        assertNotNull(search);
    }

    @Test
    public void testBuilder() {
        Search search = search().query(match("field", "value"))
                                .filter(match("field", "value"))
                                .sort(sortField("field"))
                                .build();
        assertNotNull(search);
    }

    @Test
    public void testJson() {
        Search search = search().query(match("field", "value"))
                                .filter(match("field", "value"))
                                .sort(sortField("field"))
                                .build();
        String json = search.toJson();
        assertEquals(json, Search.fromJson(json).toJson());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromJsonInvalid() {
        Search.fromJson("error");
    }

    @Test
    public void testUsesRelevance() {
        assertTrue(search().query(match("field", "value")).build().usesRelevance());
        assertFalse(search().filter(match("field", "value")).build().usesRelevance());
        assertFalse(search().sort(sortField("field")).build().usesRelevance());
        assertTrue(search().query(match("field", "value"))
                           .filter(match("field", "value"))
                           .sort(sortField("field"))
                           .build()
                           .usesRelevance());
    }

    @Test
    public void testUsesSorting() {
        assertFalse(search().query(match("field", "value")).build().usesSorting());
        assertFalse(search().filter(match("field", "value")).build().usesSorting());
        assertTrue(search().sort(sortField("field")).build().usesSorting());
        assertTrue(search().query(match("field", "value"))
                           .filter(match("field", "value"))
                           .sort(sortField("field"))
                           .build()
                           .usesRelevance());
    }

    @Test
    public void testUsesRelevanceOrSorting() {
        assertTrue(search().query(match("field", "value")).build().usesRelevanceOrSorting());
        assertFalse(search().filter(match("field", "value")).build().usesRelevanceOrSorting());
        assertTrue(search().sort(sortField("field")).build().usesRelevanceOrSorting());
        assertTrue(search().query(match("field", "value"))
                           .filter(match("field", "value"))
                           .sort(sortField("field"))
                           .build()
                           .usesRelevance());
    }

    @Test
    public void testGetSort() {
        assertNotNull(search().sort(sortField("field")).build().getSort());
        assertNull(search().query(match("field", "value")).build().getSort());
    }

    @Test
    public void testSort() {
        ColumnMapper mapper = new ColumnMapperString(true, true, true);
        mapper.init("field");
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.STANDARD.get());
        when(schema.getMapper("field")).thenReturn(mapper);
        assertNotNull(search().sort(sortField("field")).build().sort(schema));
        assertNull(search().query(match("field", "value")).build().sort(schema));
    }

    @Test
    public void testValidate() {
        ColumnMapper mapper = new ColumnMapperString(true, true, true);
        mapper.init("field");
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.STANDARD.get());
        when(schema.getMapper("field")).thenReturn(mapper);
        search().query(match("field", "value"))
                .filter(match("field", "value"))
                .sort(sortField("field"))
                .build()
                .validate(schema);
    }

    @Test
    public void testToString() {
        Search search = search().query(match("field", "value"))
                                .filter(match("field", "value"))
                                .sort(sortField("field"))
                                .build();
        assertEquals("Search{queryCondition=MatchCondition{boost=1.0, field=field, value=value}, " +
                     "filterCondition=MatchCondition{boost=1.0, field=field, value=value}, " +
                     "sort=Sort{sortFields=[SortField{field=field, reverse=false}]}}", search.toString());
    }

}
