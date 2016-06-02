/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Class for testing {@link ByteBufferUtils}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ByteBufferUtilsTest {

    @Test
    public void testBytesRef() throws Exception {
        CompositeType type = CompositeType.getInstance(UTF8Type.instance, Int32Type.instance);
        ByteBuffer in = type.decompose("monkey", 1);
        BytesRef bytesRef = ByteBufferUtils.bytesRef(in);
        ByteBuffer out = ByteBufferUtils.byteBuffer(bytesRef);
        assertEquals("Failing conversion between ByteBuffer and BytesRef", 0, ByteBufferUtil.compareUnsigned(in, out));
    }

    @Test
    public void testIsEmptyTrue() {
        ByteBuffer bb = ByteBuffer.allocate(0);
        assertTrue(ByteBufferUtils.isEmpty(bb));
    }

    @Test
    public void testIsEmptyFalse() {
        ByteBuffer bb = ByteBuffer.allocate(10);
        assertFalse(ByteBufferUtils.isEmpty(bb));
    }

    @Test
    public void testSplit() {
        ByteBuffer bb = UTF8Type.instance.decompose("test");
        assertEquals("Must be split to one element", 1, ByteBufferUtils.split(bb, UTF8Type.instance).length);
    }

    @Test
    public void testSplitComposite() {
        CompositeType type = CompositeType.getInstance(UTF8Type.instance, Int32Type.instance);
        ByteBuffer bb = type.builder()
                            .add(UTF8Type.instance.decompose("1"))
                            .add(Int32Type.instance.decompose(1))
                            .build();
        assertEquals("Must be split to two elements", 2, ByteBufferUtils.split(bb, type).length);
    }

    @Test
    public void testToString() {
        ByteBuffer bb = UTF8Type.instance.decompose("test");
        String string = ByteBufferUtils.toString(bb, UTF8Type.instance);
        assertEquals("Abstract type string conversion is failing", "test", string);
    }

    @Test
    public void testToStringComposite() throws Exception {
        CompositeType type = CompositeType.getInstance(UTF8Type.instance, Int32Type.instance);
        ByteBuffer bb = type.decompose("monkey", 1);
        String string = ByteBufferUtils.toString(bb, type);
        assertEquals("Composite type string conversion is failing", "monkey:1", string);
    }

    @Test
    public void testComposeDecompose() {
        ByteBuffer[] bbs = ByteBufferUtils.decompose(ByteBufferUtils.compose(UTF8Type.instance.decompose("test"),
                                                                             Int32Type.instance.decompose(999),
                                                                             BooleanType.instance.decompose(true)));
        assertEquals("Compose-decompose is wrong", 3, bbs.length);
        assertEquals("Compose-decompose is wrong", "test", UTF8Type.instance.compose(bbs[0]));
        assertEquals("Compose-decompose is wrong", 999, Int32Type.instance.compose(bbs[1]), 0);
        assertEquals("Compose-decompose is wrong", true, BooleanType.instance.compose(bbs[2]));
    }

    @Test
    public void testComposeDecomposeEmpty() {
        ByteBuffer[] bbs = ByteBufferUtils.decompose(ByteBufferUtils.compose());
        assertEquals("Compose-decompose with empty components is wrong", 0, bbs.length);
    }
}
