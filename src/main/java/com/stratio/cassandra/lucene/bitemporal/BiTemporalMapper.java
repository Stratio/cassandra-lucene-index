package com.stratio.cassandra.lucene.bitemporal;

import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.SortField;

/**
 * Created by eduardoalonso on 18/06/15.
 */
public class BiTemporalMapper extends ColumnMapper {
    /**
     * Builds a new {@link ColumnMapper} supporting the specified types for indexing.
     *
     * @param name           The name of the mapper.
     * @param indexed        If the field supports searching.
     * @param sorted         If the field supports sorting.
     * @param supportedTypes The supported Cassandra types for indexing.
     */
    protected BiTemporalMapper(String name, Boolean indexed, Boolean sorted, AbstractType<?>... supportedTypes) {
        super(name, indexed, sorted, supportedTypes);
    }

    @Override
    public void addFields(Document document, Columns columns) {

    }

    @Override
    public SortField sortField(boolean reverse) {
        return null;
    }

    @Override
    public void validate(CFMetaData metaData) {

    }
}
