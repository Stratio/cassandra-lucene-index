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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.junit.Test;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class PartitionKeyMapperTest {

    @Test
    public void testConstructorPartitionKeyMapper() throws InvalidRequestException, ConfigurationException {
            /* need to mock Storage. getPartitoner
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

            CFMetaData metadata = CFMetaData.fromThrift(cfDef);
            Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();


            PartitionKeyMapper partitionKeyMapper=PartitionKeyMapper.instance(metadata,schema);

            assertEquals("clustering KeyMapper ",partitionKeyMapper.getType(),metadata.comparator);
            */
    }
}
