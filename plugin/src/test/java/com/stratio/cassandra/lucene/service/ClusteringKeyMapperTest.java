/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.service;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.SchemaBuilders;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.stringMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.textMapper;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class ClusteringKeyMapperTest {

    private static final Double REFRESH_SECONDS = 0.1D;
    private static final int REFRESH_MILLISECONDS = (int) (REFRESH_SECONDS * 1000);
    private static final int WAIT_MILLISECONDS = REFRESH_MILLISECONDS * 2;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testCRUD() throws IOException, InterruptedException, InvalidRequestException, ConfigurationException {
        List<ColumnDef> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field1"),
                                            UTF8Type.class.getCanonicalName()).setIndex_name("field1")
                                                                              .setIndex_type(IndexType.KEYS));

        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field2"),
                                            IntegerType.class.getCanonicalName()).setIndex_name("field2")
                                                                                 .setIndex_type(IndexType.KEYS));
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setColumn_metadata(columnDefinitions)
                                 .setKeyspace("Keyspace1")
                                 .setName("Standard1");

        CFMetaData metadata = ThriftConversion.fromThrift(cfDef);
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();

        ClusteringKeyMapper clusteringKeyMapper = ClusteringKeyMapper.instance(metadata, schema);

        assertEquals("clustering KeyMapper ", clusteringKeyMapper.getType(), metadata.comparator);
    }

    @Test
    public void testAddFields() throws InvalidRequestException, ConfigurationException {
        List<ColumnDef> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field1"),
                                            UTF8Type.class.getCanonicalName()).setIndex_name("field1")
                                                                              .setIndex_type(IndexType.KEYS));

        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field2"),
                                            IntegerType.class.getCanonicalName()).setIndex_name("field2")
                                                                                 .setIndex_type(IndexType.KEYS));
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setColumn_metadata(columnDefinitions)
                                 .setKeyspace("Keyspace1")
                                 .setName("Standard1");

        CFMetaData metadata = ThriftConversion.fromThrift(cfDef);
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();
        ClusteringKeyMapper clusteringKeyMapper = ClusteringKeyMapper.instance(metadata, schema);

        CellName cellName = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        Document doc = new Document();

        clusteringKeyMapper.addFields(doc, cellName);
        Field field = (Field) doc.getField(ClusteringKeyMapper.FIELD_NAME);
        assertNotNull("clusteringKeyMapper addFields to Document must add al least one Field to Doc", field);
        assertEquals("clusteringKeyMapper.byteRef included in Document must be equal",
                     clusteringKeyMapper.bytesRef(cellName),
                     field.binaryValue());

    }

    @Test
    public void testClusteringKeyFromColumnFamily() throws InvalidRequestException, ConfigurationException {
        List<ColumnDef> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field1"),
                                            UTF8Type.class.getCanonicalName()).setIndex_name("field1")
                                                                              .setIndex_type(IndexType.KEYS));

        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field2"),
                                            IntegerType.class.getCanonicalName()).setIndex_name("field2")
                                                                                 .setIndex_type(IndexType.KEYS));
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setColumn_metadata(columnDefinitions)
                                 .setKeyspace("Keyspace1")
                                 .setName("Standard1");

        CFMetaData metadata = ThriftConversion.fromThrift(cfDef);
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();
        ClusteringKeyMapper clusteringKeyMapper = ClusteringKeyMapper.instance(metadata, schema);

    }

    @Test
    public void testClusteringKeyFromDocument() throws InvalidRequestException, ConfigurationException {

        List<ColumnDef> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field1"),
                                            UTF8Type.class.getCanonicalName()).setIndex_name("field1")
                                                                              .setIndex_type(IndexType.KEYS));

        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field2"),
                                            IntegerType.class.getCanonicalName()).setIndex_name("field2")
                                                                                 .setIndex_type(IndexType.KEYS));
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setColumn_metadata(columnDefinitions)
                                 .setKeyspace("Keyspace1")
                                 .setName("Standard1");

        CFMetaData metadata = ThriftConversion.fromThrift(cfDef);
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();
        ClusteringKeyMapper clusteringKeyMapper = ClusteringKeyMapper.instance(metadata, schema);

        CellName cellName = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        Document doc = new Document();

        clusteringKeyMapper.addFields(doc, cellName);

        CellName cellName2 = clusteringKeyMapper.clusteringKey(doc);

        assertEquals("CellName added to Document must be equal like returned by clusteringKeymapper.clusteringkey" +
                     "(doc)", cellName, cellName2);

    }

    @Test
    public void testClusteringKeyFromByteRef() throws InvalidRequestException, ConfigurationException {
        List<ColumnDef> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field1"),
                                            UTF8Type.class.getCanonicalName()).setIndex_name("field1")
                                                                              .setIndex_type(IndexType.KEYS));

        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field2"),
                                            IntegerType.class.getCanonicalName()).setIndex_name("field2")
                                                                                 .setIndex_type(IndexType.KEYS));
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setColumn_metadata(columnDefinitions)
                                 .setKeyspace("Keyspace1")
                                 .setName("Standard1");

        CFMetaData metadata = ThriftConversion.fromThrift(cfDef);
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();
        ClusteringKeyMapper clusteringKeyMapper = ClusteringKeyMapper.instance(metadata, schema);
        CellName cellName = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));

        BytesRef bytesRef = clusteringKeyMapper.bytesRef(cellName);
        CellName cellName2 = clusteringKeyMapper.clusteringKey(bytesRef);
        assertEquals("clusteringKeyMapper.clusteringKey(bytesRef(cellName)) must be equal to cellName",
                     cellName,
                     cellName2);
    }

    @Test
    public void testClusteringÇKeyFromRow() {

    }
}
