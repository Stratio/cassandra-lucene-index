package com.stratio.cassandra.lucene.testsAT.varia;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.Bytes;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.datastax.driver.core.LocalDate.fromYearMonthDay;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.utils.UUIDs.random;
import static com.datastax.driver.core.utils.UUIDs.timeBased;
import static com.stratio.cassandra.lucene.builder.Builder.all;
import static com.stratio.cassandra.lucene.builder.Builder.integerMapper;
import static java.time.LocalTime.now;
import static java.util.Calendar.DAY_OF_MONTH;
import static org.junit.Assert.assertEquals;

/**
 * Test clustering range combined with luceen queries
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class CombinedClusteringRangeIT extends BaseIT {
    private static final String KEYSPACE_PREFIX = "combined_clustering_ranges";
    private static final int NUM_PARTITIONS = 10;
    private static final int PARTITION_SIZE = 1000;

    private CassandraUtils createBasicTable(boolean ascending, String basicType) {
        return CassandraUtils.builder(KEYSPACE_PREFIX)
                             .withUseNewQuerySyntax(true)
                             .withPartitionKey("p_key")
                             .withClusteringKey("c_key")
                             .withClusteringOrder("c_key", ascending)
                             .withColumn("p_key", "int", integerMapper())
                             .withColumn("c_key", basicType)
                             .build()
                             .createKeyspace()
                             .createTable();
    }

    private void assertLuceneQueryOutputLikeCassandra(CassandraUtils utils,
                                                      Object p_key,
                                                      Object c_key_gt,
                                                      Object c_key_lt,
                                                      Class<?> returnType) {
        List<Row> rows = utils.refresh()
                              .filter(all())
                              .andEq("p_key", p_key)
                              .andGt("c_key", c_key_gt)
                              .andLt("c_key", c_key_lt)
                              .refresh(true)
                              .fetchSize(Integer.MAX_VALUE).get();

        List<Object> clustKeys = rows.stream()
                                     .map((row) -> row.get("c_key", returnType))
                                     .collect(Collectors.toList());

        List<Row> sorted = utils.refresh().execute(select().all()
                                                           .from(utils.getKeyspace(), utils.getTable())
                                                           .where(QueryBuilder.eq("p_key", p_key))
                                                           .and(QueryBuilder.gt("c_key", c_key_gt))
                                                           .and(QueryBuilder.lt("c_key", c_key_lt))).all();

        List<Object> sortedClustKeys = sorted.stream()
                                             .map((row) -> row.get("c_key", returnType))
                                             .collect(Collectors.toList());

        utils.dropIndex().dropTable().dropKeyspace();
        assertEquals(sortedClustKeys, clustKeys);
    }

    private Long getNanoFromTime(LocalTime time) {
        Long SEG_TO_NANO = 1000000000L;
        Long MIN_TO_NANO = SEG_TO_NANO * 60L;
        Long HOUR_TO_NANO = MIN_TO_NANO * 60L;
        Long nanos = 0L;
        nanos += time.getNano();
        nanos += time.getSecond() * SEG_TO_NANO;
        nanos += time.getMinute() * MIN_TO_NANO;
        nanos += time.getHour() * HOUR_TO_NANO;
        return nanos;
    }

    @Test
    public void testSortedDocValuesByAscii() {
        CassandraUtils utils = createBasicTable(true, "ascii");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, Integer.toString(j)});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, "50", "743", String.class);
    }

    @Test
    public void testSortedDocValuesByInverseAscii() {
        CassandraUtils utils = createBasicTable(false, "ascii");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, Integer.toString(j)});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, "50", "743", String.class);
    }

    @Test
    public void testSortedDocValuesByBigInt() {
        CassandraUtils utils = createBasicTable(true, "bigint");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, new Long(j)});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152L, 267L, Long.class);
    }

    @Test
    public void testSortedDocValuesByInverseBigInt() {
        CassandraUtils utils = createBasicTable(false, "bigint");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, new Long(j)});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152L, 267L, Long.class);
    }

    @Test
    public void testSortedDocValuesByBlob() {
        CassandraUtils utils = createBasicTable(true, "blob");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"},
                             new Object[]{i, ByteBuffer.wrap(Bytes.fromRawHexString(Long.toString(j), 0))});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils,
                                             1,
                                             ByteBuffer.wrap(Bytes.fromRawHexString("0000",0)),
                                             ByteBuffer.wrap(Bytes.fromRawHexString("ffff",0)),
                                             ByteBuffer.class);
    }

    @Test
    public void testSortedDocValuesByInverseBlob() {
        CassandraUtils utils = createBasicTable(false, "blob");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"},
                             new Object[]{i, ByteBuffer.wrap(Bytes.fromRawHexString(Long.toString(j), 0))});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils,
                                             1,
                                             ByteBuffer.wrap(Bytes.fromRawHexString("0000",0)),
                                             ByteBuffer.wrap(Bytes.fromRawHexString("ffff",0)),
                                             ByteBuffer.class);
    }

    @Test
    public void testSortedDocValuesByBoolean() {
        CassandraUtils utils = createBasicTable(true, "boolean");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, true})
                 .insert(new String[]{"p_key", "c_key"}, new Object[]{i, false});
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, true, false, Boolean.class);
    }

    @Test
    public void testSortedDocValuesByInverseBoolean() {
        CassandraUtils utils = createBasicTable(false, "boolean");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, true})
                 .insert(new String[]{"p_key", "c_key"}, new Object[]{i, false});
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, true, false, Boolean.class);
    }

    @Test
    public void testSortedDocValuesByDate() {
        CassandraUtils utils = createBasicTable(true, "date");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            LocalDate date = fromYearMonthDay(2016, 1, 1);
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, date});
                date = date.add(DAY_OF_MONTH, 5);
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils,
                                             1,
                                             LocalDate.fromYearMonthDay(2016, 1, 1),
                                             LocalDate.fromYearMonthDay(2020, 1, 1),
                                             LocalDate.class);
    }

    @Test
    public void testSortedDocValuesByInverseDate() {
        CassandraUtils utils = createBasicTable(false, "date");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            LocalDate date = fromYearMonthDay(2016, 1, 1);
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, date});
                date = date.add(DAY_OF_MONTH, 5);
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils,
                                             1,
                                             LocalDate.fromYearMonthDay(2016, 1, 1),
                                             LocalDate.fromYearMonthDay(2020, 1, 1),
                                             LocalDate.class);
    }

    @Test
    public void testSortedDocValuesByDecimal() {
        CassandraUtils utils = createBasicTable(true, "decimal");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (float j = 0.0f; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152.0f, 267.56f, BigDecimal.class);
    }

    @Test
    public void testSortedDocValuesByInverseDecimal() {
        CassandraUtils utils = createBasicTable(false, "decimal");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (float j = 0.0f; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152.0f, 267.56f, BigDecimal.class);
    }

    @Test
    public void testSortedDocValuesByDouble() {
        CassandraUtils utils = createBasicTable(true, "double");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (double j = 0.0f; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152.0, 267.56, Double.class);
    }

    @Test
    public void testSortedDocValuesByInverseDouble() {
        CassandraUtils utils = createBasicTable(false, "double");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (double j = 0.0f; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152.0, 267.56, Double.class);
    }

    @Test
    public void testSortedDocValuesByFloat() {
        CassandraUtils utils = createBasicTable(true, "float");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (float j = 0.0f; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152.0F, 267.56F, Float.class);
    }

    @Test
    public void testSortedDocValuesByInverseFloat() {
        CassandraUtils utils = createBasicTable(false, "float");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (float j = 0.0f; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152.0F, 267.56F, Float.class);
    }

    @Test
    public void testSortedDocValuesByInet() throws UnknownHostException {
        CassandraUtils utils = createBasicTable(true, "inet");
        String commonPrefixInet = "192.168.1.";
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < 256; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, InetAddress.getByName(commonPrefixInet + Integer.toString(j))});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, InetAddress.getByName( "192.168.1.0"), InetAddress.getByName("192.168.1.255"), InetAddress.class);
    }

    @Test
    public void testSortedDocValuesByInverseInet() throws UnknownHostException {
        CassandraUtils utils = createBasicTable(false, "inet");
        String commonPrefixInet = "192.168.1.";
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < 256; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, InetAddress.getByName(commonPrefixInet + Integer.toString(j))});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, InetAddress.getByName("192.168.1.0"), InetAddress.getByName("192.168.1.255"), InetAddress.class);
    }

    @Test
    public void testSortedDocValuesByInt() {
        CassandraUtils utils = createBasicTable(true, "int");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152, 267, Integer.class);
    }

    @Test
    public void testSortedDocValuesByInverseInt() {
        CassandraUtils utils = createBasicTable(false, "int");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152, 267, Integer.class);
    }

    @Test
    public void testSortedDocValuesBySmallint() {
        CassandraUtils utils = createBasicTable(true, "smallint");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (short j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, (short) 152, (short) 267, Short.class);
    }

    @Test
    public void testSortedDocValuesByInverseSmallint() {
        CassandraUtils utils = createBasicTable(false, "smallint");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152, 267, Short.class);
    }

    @Test
    public void testSortedDocValuesByVarint() {
        CassandraUtils utils = createBasicTable(true, "varint");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (short j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152, 267, BigInteger.class);
    }

    @Test
    public void testSortedDocValuesByInverseVarint() {
        CassandraUtils utils = createBasicTable(false, "varint");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152, 267, BigInteger.class);
    }

    @Test
    public void testSortedDocValuesByTinyint() {
        CassandraUtils utils = createBasicTable(true, "tinyint");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (Byte j = Byte.MIN_VALUE; j < Byte.MAX_VALUE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, Byte.MIN_VALUE, (byte) 0x00, Byte.class);
    }

    @Test
    public void testSortedDocValuesByInverseTinyint() {
        CassandraUtils utils = createBasicTable(false, "tinyint");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (Byte j = Byte.MIN_VALUE; j < Byte.MAX_VALUE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, j});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, Byte.MIN_VALUE, (byte) 0x00, Byte.class);
    }

    @Test
    public void testSortedDocValuesByText() {
        CassandraUtils utils = createBasicTable(true, "text");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, Integer.toString(j)});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, "50", "743", String.class);
    }

    @Test
    public void testSortedDocValuesByInverseText() {
        CassandraUtils utils = createBasicTable(false, "text");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, Integer.toString(j)});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, "50", "743", String.class);
    }

    @Test
    public void testSortedDocValuesByTime() {
        CassandraUtils utils = createBasicTable(true, "time");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            LocalTime time = now();
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, getNanoFromTime(time)});
                time = time.plusMinutes(5);
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152L, 267L, Long.class);
    }

    @Test
    public void testSortedDocValuesByInverseTime() {
        CassandraUtils utils = createBasicTable(false, "time");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            LocalTime time = now();
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, getNanoFromTime(time)});
                time = time.plusMinutes(5);
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152L, 267L, Long.class);
    }

    @Test
    public void testSortedDocValuesByTimestamp() {
        CassandraUtils utils = createBasicTable(true, "time");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            Date date = new Date();
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, date});
                date.setTime(date.getTime() + 20L);
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152L, 267L, Long.class);
    }

    @Test
    public void testSortedDocValuesByInverseTimestamp() {
        CassandraUtils utils = createBasicTable(false, "time");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            Date date = new Date();
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, date});
                date.setTime(date.getTime() + 20L);
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, 152L, 267L, Long.class);
    }

    @Test
    public void testSortedDocValuesByUUID() {
        CassandraUtils utils = createBasicTable(true, "uuid");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, random()});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, random(), random(), UUID.class);
    }

    @Test
    public void testSortedDocValuesByInverseUUID() {
        CassandraUtils utils = createBasicTable(false, "uuid");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, random()});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, random(), random(), UUID.class);
    }

    @Test
    public void testSortedDocValuesByTimeUUID() {
        CassandraUtils utils = createBasicTable(true, "uuid");
        UUID first = timeBased(), last = timeBased();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                UUID actual = timeBased();
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, actual});
                if (i == 1) {
                    if (j == 0) {
                        first = actual;
                    } else if (j == (PARTITION_SIZE - 1)) {
                        last = actual;
                    }
                }
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, first, last, UUID.class);
    }

    @Test
    public void testSortedDocValuesByInverseTimeUUID() {
        CassandraUtils utils = createBasicTable(false, "uuid");
        UUID first = timeBased(), last = timeBased();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                UUID actual = timeBased();
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, actual});
                if (i == 1) {
                    if (j == 0) {
                        first = actual;
                    } else if (j == (PARTITION_SIZE - 1)) {
                        last = actual;
                    }
                }
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, first, last, UUID.class);
    }

    @Test
    public void testSortedDocValuesByVarchar() {
        CassandraUtils utils = createBasicTable(true, "varchar");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, Integer.toString(j)});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, "50", "743", String.class);
    }

    @Test
    public void testSortedDocValuesByInverseVarchar() {
        CassandraUtils utils = createBasicTable(false, "varchar");
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"p_key", "c_key"}, new Object[]{i, Integer.toString(j)});
            }
        }
        utils.createIndex();
        assertLuceneQueryOutputLikeCassandra(utils, 1, "50", "743", String.class);
    }

    @Test
    public void testPartitionKeyComposedByTwoColumns() {
        CassandraUtils utils = CassandraUtils.builder(KEYSPACE_PREFIX)
                              .withPartitionKey("p_key_1","p_key_2")
                              .withClusteringKey("c_key")
                              .withColumn("p_key_1", "int")
                              .withColumn("p_key_2", "int")
                              .withColumn("c_key", "text")
                              .withClusteringOrder("c_key", true)
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex();

        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < NUM_PARTITIONS; j++) {
                for (int k = 0; k < PARTITION_SIZE; k++) {
                    utils.insert(new String[]{"p_key_1", "p_key_2", "c_key"}, new Object[]{i, j, Integer.toString(k)});
                }
            }
        }
        List<Row> rows = utils.refresh()
                              .filter(all())
                              .andEq("p_key_1", NUM_PARTITIONS/3)
                              .andEq("p_key_2",NUM_PARTITIONS/2)
                              .andGt("c_key", Integer.toString(PARTITION_SIZE/6))
                              .andLt("c_key", Integer.toString(PARTITION_SIZE-1)).fetchSize(Integer.MAX_VALUE).get();

        List<Object> clustKeys = rows.stream()
                                     .map((row) -> row.get("c_key", String.class))
                                     .collect(Collectors.toList());

        List<Row> sorted = utils.refresh().execute(select().all()
                                                           .from(utils.getKeyspace(), utils.getTable())
                                                           .where(QueryBuilder.eq("p_key_1", NUM_PARTITIONS/3))
                                                           .and(QueryBuilder.eq("p_key_2", NUM_PARTITIONS/2))
                                                           .and(QueryBuilder.gt("c_key", Integer.toString(PARTITION_SIZE/6)))
                                                           .and(QueryBuilder.lt("c_key", Integer.toString(PARTITION_SIZE-1)))).all();

        List<Object> sortedClustKeys = sorted.stream()
                                             .map((row) -> row.get("c_key", String.class))
                                             .collect(Collectors.toList());

        utils.dropIndex().dropTable().dropKeyspace();
        assertEquals("DocValues sorting does not work well with partition key compound by two columns.", sortedClustKeys, clustKeys);
    }

    @Test
    public void testPartitionKeyComposedByThreeColumns() {
        CassandraUtils utils = CassandraUtils.builder(KEYSPACE_PREFIX)
                                             .withPartitionKey("p_key_1","p_key_2","p_key_3")
                                             .withClusteringKey("c_key")
                                             .withColumn("p_key_1", "int")
                                             .withColumn("p_key_2", "int")
                                             .withColumn("p_key_3", "int")
                                             .withColumn("c_key", "text")
                                             .withClusteringOrder("c_key", true)
                                             .build()
                                             .createKeyspace()
                                             .createTable();

        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < NUM_PARTITIONS; j++) {
                for (int k = 0; k < NUM_PARTITIONS; k++) {
                    for (int l = 0; l < PARTITION_SIZE; l++) {
                        utils.insert(new String[]{"p_key_1", "p_key_2", "p_key_3", "c_key"},
                                     new Object[]{i, j, k, Integer.toString(l)});
                    }
                }
            }
        }
        List<Row> rows = utils.createIndex()
                              .refresh()
                              .filter(all())
                              .andEq("p_key_1", NUM_PARTITIONS/3)
                              .andEq("p_key_2",NUM_PARTITIONS/2)
                              .andEq("p_key_3",NUM_PARTITIONS/4)
                              .andGt("c_key", Integer.toString(PARTITION_SIZE/6))
                              .andLt("c_key", Integer.toString(PARTITION_SIZE-1)).fetchSize(Integer.MAX_VALUE).get();

        List<Object> clustKeys = rows.stream()
                                     .map((row) -> row.get("c_key", String.class))
                                     .collect(Collectors.toList());

        List<Row> sorted = utils.refresh().execute(select().all()
                                                           .from(utils.getKeyspace(), utils.getTable())
                                                           .where(QueryBuilder.eq("p_key_1", NUM_PARTITIONS/3))
                                                           .and(QueryBuilder.eq("p_key_2", NUM_PARTITIONS/2))
                                                           .and(QueryBuilder.eq("p_key_3", NUM_PARTITIONS/4))
                                                           .and(QueryBuilder.gt("c_key", Integer.toString(PARTITION_SIZE/6)))
                                                           .and(QueryBuilder.lt("c_key", Integer.toString(PARTITION_SIZE-1)))).all();

        List<Object> sortedClustKeys = sorted.stream()
                                             .map((row) -> row.get("c_key", String.class))
                                             .collect(Collectors.toList());

        utils.dropIndex().dropTable().dropKeyspace();
        assertEquals("DocValues sorting does not work well with partition key compound by three columns.", sortedClustKeys, clustKeys);
    }

    @Test
    public void testNonCompleteClusteringRangeOne() {
        CassandraUtils utils = CassandraUtils.builder(KEYSPACE_PREFIX)
                                             .withPartitionKey("p_key")
                                             .withClusteringKey("c_key_1", "c_key_2", "c_key_3")
                                             .withColumn("p_key", "int")
                                             .withColumn("c_key_1", "text")
                                             .withColumn("c_key_2", "timeuuid")
                                             .withColumn("c_key_3", "float")
                                             .withClusteringOrder(new String[]{"c_key_1","c_key_2","c_key_3"}, new Boolean[]{false,true,false})
                                             .build()
                                             .createKeyspace()
                                             .createTable();

        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j= 0; j< 20; j++) {
                UUID time= timeBased();
                utils.insert(new String[]{"p_key", "c_key_1", "c_key_2", "c_key_3"},
                             new Object[]{i, Integer.toString(20-j), time, (float)j});
            }
        }
        List<Row> rows = utils.createIndex()
                              .refresh()
                              .filter(all())
                              .andEq("p_key", NUM_PARTITIONS/2)
                              .andGt("c_key_1", Integer.toString(PARTITION_SIZE/6))
                              .andLt("c_key_1", Integer.toString(PARTITION_SIZE-1)).fetchSize(Integer.MAX_VALUE).get();

        List<Object> clustKeys = rows.stream()
                                     .map((row) -> row.get("c_key_1", String.class))
                                     .collect(Collectors.toList());

        List<Row> sorted = utils.refresh().execute(select().all()
                                                           .from(utils.getKeyspace(), utils.getTable())
                                                           .where(QueryBuilder.eq("p_key", NUM_PARTITIONS/2))
                                                           .and(QueryBuilder.gt("c_key_1", Integer.toString(PARTITION_SIZE/6)))
                                                           .and(QueryBuilder.lt("c_key_1", Integer.toString(PARTITION_SIZE-1)))).all();

        List<Object> sortedClustKeys = sorted.stream()
                                             .map((row) -> row.get("c_key_1", String.class))
                                             .collect(Collectors.toList());

        utils.dropIndex().dropTable().dropKeyspace();
        assertEquals("DocValues sorting does not work well with subrange (1/3) of clusteringKeys.", sortedClustKeys, clustKeys);

    }

    //TODO three partition keys
    @Test
    public void testNonCompleteClusteringRangeTwo() {
        CassandraUtils utils = CassandraUtils.builder(KEYSPACE_PREFIX)
                                             .withPartitionKey("p_key")
                                             .withClusteringKey("c_key_1", "c_key_2", "c_key_3")
                                             .withColumn("p_key", "int")
                                             .withColumn("c_key_1", "text")
                                             .withColumn("c_key_2", "timeuuid")
                                             .withColumn("c_key_3", "float")
                                             .withClusteringOrder(new String[]{"c_key_1","c_key_2","c_key_3"}, new Boolean[]{false,true,false})
                                             .build()
                                             .createKeyspace()
                                             .createTable();

        UUID first=timeBased(), last=timeBased();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j= 0; j< 20; j++) {
                UUID time= timeBased();
                utils.insert(new String[]{"p_key", "c_key_1", "c_key_2", "c_key_3"},
                             new Object[]{i, Integer.toString(20-j), time, (float)j});
                if ((i==NUM_PARTITIONS/2) && (j==0)) {
                    first=time;
                }
                if ((i==NUM_PARTITIONS/2) && (j==19)) {
                    last=time;
                }
            }
        }
        List<Row> rows = utils.createIndex()
                              .refresh()
                              .filter(all())
                              .andEq("p_key", NUM_PARTITIONS/2)
                              .andEq("c_key_1", Integer.toString(PARTITION_SIZE/6))
                              .andGt("c_key_2", first)
                              .andLt("c_key_2", last)
                              .fetchSize(Integer.MAX_VALUE).get();

        List<Object> clustKeys = rows.stream()
                                     .map((row) -> row.get("c_key_1", String.class))
                                     .collect(Collectors.toList());

        List<Row> sorted = utils.refresh().execute(select().all()
                                                           .from(utils.getKeyspace(), utils.getTable())
                                                           .where(QueryBuilder.eq("p_key", NUM_PARTITIONS/2))
                                                           .and(QueryBuilder.eq("c_key_1", Integer.toString(PARTITION_SIZE/6)))
                                                           .and(QueryBuilder.gt("c_key_2", first))
                                                           .and(QueryBuilder.lt("c_key_2", last))).all();

        List<Object> sortedClustKeys = sorted.stream()
                                             .map((row) -> row.get("c_key_1", String.class))
                                             .collect(Collectors.toList());

        utils.dropIndex().dropTable().dropKeyspace();
        assertEquals("DocValues sorting does not work well with subrange (2/3) of clusteringKeys.", sortedClustKeys, clustKeys);
    }

    //TODO three partition keys
    @Test
    public void testNonCompleteClusteringRangeThree() {
        CassandraUtils utils = CassandraUtils.builder(KEYSPACE_PREFIX)
                                             .withPartitionKey("p_key")
                                             .withClusteringKey("c_key_1", "c_key_2", "c_key_3")
                                             .withColumn("p_key", "int")
                                             .withColumn("c_key_1", "text")
                                             .withColumn("c_key_2", "timeuuid")
                                             .withColumn("c_key_3", "float")
                                             .withClusteringOrder(new String[]{"c_key_1","c_key_2","c_key_3"}, new Boolean[]{false,true,false})
                                             .build()
                                             .createKeyspace()
                                             .createTable();

        UUID first=timeBased(), last=timeBased();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j= 0; j< 20; j++) {
                UUID time= timeBased();
                utils.insert(new String[]{"p_key", "c_key_1", "c_key_2", "c_key_3"},
                             new Object[]{i, Integer.toString(20-j), time, (float)j});
                if ((i==NUM_PARTITIONS/2) && (j==0)) {
                    first=time;
                }
                if ((i==NUM_PARTITIONS/2) && (j==19)) {
                    last=time;
                }
            }
        }
        List<Row> rows = utils.createIndex()
                              .refresh()
                              .filter(all())
                              .andEq("p_key", NUM_PARTITIONS/2)
                              .andEq("c_key_1", Integer.toString(PARTITION_SIZE/6))
                              .andEq("c_key_2", first)
                              .andGt("c_key_3", 0.0F)
                              .andLt("c_key_3", 20.0F)
                              .fetchSize(Integer.MAX_VALUE).get();

        List<Object> clustKeys = rows.stream()
                                     .map((row) -> row.get("c_key_1", String.class))
                                     .collect(Collectors.toList());

        List<Row> sorted = utils.refresh().execute(select().all()
                                                           .from(utils.getKeyspace(), utils.getTable())
                                                           .where(QueryBuilder.eq("p_key", NUM_PARTITIONS/2))
                                                           .and(QueryBuilder.eq("c_key_1", Integer.toString(PARTITION_SIZE/6)))
                                                           .and(QueryBuilder.eq("c_key_2", first))
                                                           .and(QueryBuilder.gt("c_key_3", 0.0F))
                                                           .and(QueryBuilder.lt("c_key_3", 20.0F))).all();

        List<Object> sortedClustKeys = sorted.stream()
                                             .map((row) -> row.get("c_key_1", String.class))
                                             .collect(Collectors.toList());

        utils.dropIndex().dropTable().dropKeyspace();
        assertEquals("DocValues sorting does not work well with subrange (2/3) of clusteringKeys.", sortedClustKeys, clustKeys);
    }


    //TODO three partition keys
    @Test
    public void testNonCompleteClusteringRangeOneWithComplexOrder() {
    }

    //TODO three partition keys
    @Test
    public void testNonCompleteClusteringRangeTwoWithComplexOrder() {
    }

    //TODO three partition keys
    @Test
    public void testNonCompleteClusteringRangeThreeWithComplexOrder() {
    }
/*
    //TODO three partition keys
    @Test
    public void testClusteringRangeWithTokenUDF() {

        CassandraUtils utils = CassandraUtils.builder(KEYSPACE_PREFIX)
                                             .withPartitionKey("p_key")
                                             .withClusteringKey("c_key_1", "c_key_2", "c_key_3")
                                             .withColumn("p_key", "int")
                                             .withColumn("c_key_1", "text")
                                             .withColumn("c_key_2", "timeuuid")
                                             .withColumn("c_key_3", "float")
                                             .withClusteringOrder(new String[]{"c_key_1","c_key_2","c_key_3"}, new Boolean[]{false,true,false})
                                             .build()
                                             .createKeyspace()
                                             .createTable()
                                             .createIndex();

       UUID first=timeBased(), last=timeBased();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j= 0; j< 20; j++) {
                UUID time= timeBased();
                utils.insert(new String[]{"p_key", "c_key_1", "c_key_2", "c_key_3"},
                             new Object[]{i, Integer.toString(20-j), time, (float)j});
                if ((i==NUM_PARTITIONS/2) && (j==0)) {
                    first=time;
                }
                if ((i==NUM_PARTITIONS/2) && (j==19)) {
                    last=time;
                }
            }
        }
        List<Row> rows = utils.refresh()
                              .filter(all())
                              .andEq("p_key", NUM_PARTITIONS/2)
                              .andEq("c_key_1", Integer.toString(PARTITION_SIZE/6))
                              .andEq("c_key_2", first)
                              .andGt("c_key_3", 0.0F)
                              .andLt("c_key_3", 20.0L0F)
                              .andGt("token(p_key)",-9223372036854775808L)
                              .andLt("token(p_key)",-3074457345618258603L)
                              .fetchSize(Integer.MAX_VALUE).get();

        List<Object> clustKeys = rows.stream()
                                     .map((row) -> row.get("c_key_1", String.class))
                                     .collect(Collectors.toList());

        List<Row> sorted = utils.refresh().execute(select().all()
                                                           .from(utils.getKeyspace(), utils.getTable())
                                                           .where(QueryBuilder.eq("p_key", NUM_PARTITIONS/2))
                                                           .and(QueryBuilder.eq("c_key_1", Integer.toString(PARTITION_SIZE/6)))
                                                           .and(QueryBuilder.eq("c_key_2", first))
                                                           .and(QueryBuilder.lt("c_key_3", 0.0F))
                                                           .and(QueryBuilder.lt("c_key_3", 20.0F))).all();

        List<Object> sortedClustKeys = sorted.stream()
                                             .map((row) -> row.get("c_key_1", String.class))
                                             .collect(Collectors.toList());

        utils.dropIndex().dropTable().dropKeyspace();
        assertEquals("DocValues sorting does not work well with subrange (2/3) of clusteringKeys.", sortedClustKeys, clustKeys);
    }*/

}
