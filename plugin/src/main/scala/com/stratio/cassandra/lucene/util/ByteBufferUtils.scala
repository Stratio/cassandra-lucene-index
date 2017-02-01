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
package com.stratio.cassandra.lucene.util

import java.nio.ByteBuffer

import org.apache.cassandra.db.marshal.{AbstractType, CompositeType}
import org.apache.cassandra.utils.ByteBufferUtil.{readShortLength, writeShortLength}
import org.apache.cassandra.utils.{ByteBufferUtil, Hex}
import org.apache.lucene.util.BytesRef

import scala.annotation.varargs


/** Utility class with some [[ByteBuffer]] transformation utilities.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
object ByteBufferUtils {

  /** Returns the specified [[ByteBuffer]] as a byte array.
    *
    * @param bb a [[ByteBuffer]] to be converted to a byte array
    * @return the byte array representation of `bb`
    */
  def asArray(bb: ByteBuffer): Array[Byte] = {
    val duplicate = bb.duplicate
    val bytes = new Array[Byte](duplicate.remaining)
    duplicate.get(bytes)
    bytes
  }

  /** Returns `true` if the specified [[ByteBuffer]] is empty, `false` otherwise.
    *
    * @param byteBuffer the byte buffer
    * @return `true` if the specified [[ByteBuffer]] is empty, `false` otherwise.
    */
  def isEmpty(byteBuffer: ByteBuffer): Boolean = byteBuffer.remaining == 0

  /** Returns the [[ByteBuffer]]s contained in the specified byte buffer according to the specified
    * type.
    *
    * @param byteBuffer the byte buffer to be split
    * @param type       the  type of the byte buffer
    * @return the byte buffers contained in `byteBuffer` according to `type`
    */
  def split(byteBuffer: ByteBuffer, `type`: AbstractType[_]): Array[ByteBuffer] = `type` match {
    case c: CompositeType => c.split(byteBuffer)
    case _ => Array[ByteBuffer](byteBuffer)
  }

  /** Returns the hexadecimal [[String]] representation of the specified [[ByteBuffer]].
    *
    * @param byteBuffer a [[ByteBuffer]]
    * @return the hexadecimal `string` representation of `byteBuffer`
    */
  def toHex(byteBuffer: ByteBuffer): String = {
    if (byteBuffer == null) null else ByteBufferUtil.bytesToHex(byteBuffer)
  }

  /** Returns the hexadecimal [[String]] representation of the specified [[BytesRef]].
    *
    * @param bytesRef a [[BytesRef]]
    * @return the hexadecimal `String` representation of `bytesRef`
    */
  def toHex(bytesRef: BytesRef): String = ByteBufferUtil.bytesToHex(byteBuffer(bytesRef))

  /** Returns the hexadecimal [[String]] representation of the specified [[Byte]]s.
    *
    * @param bytes the bytes
    * @return The hexadecimal `String` representation of `bytes`
    */
  def toHex(bytes: Byte*): String = toHex(bytes.toArray)

  /** Returns the hexadecimal [[String]] representation of the specified [[Byte]] array.
    *
    * @param bytes the byte array
    * @return The hexadecimal `String` representation of `bytes`
    */
  def toHex(bytes: Array[Byte]): String = Hex.bytesToHex(bytes, 0, bytes.length)

  /** Returns the hexadecimal [[String]] representation of the specified [[Byte]].
    *
    * @param b the byte
    * @return the hexadecimal `String` representation of `b`
    */
  def toHex(b: Byte): String = Hex.bytesToHex(b)

  /** Returns the [[BytesRef]] representation of the specified [[ByteBuffer]].
    *
    * @param bb the byte buffer
    * @return the [[BytesRef]] representation of the byte buffer
    */
  def bytesRef(bb: ByteBuffer): BytesRef = new BytesRef(asArray(bb))

  /** Returns the [[ByteBuffer]] representation of the specified [[BytesRef]].
    *
    * @param bytesRef the [[BytesRef]]
    * @return the [[ByteBuffer]] representation of `bytesRef`
    */
  def byteBuffer(bytesRef: BytesRef): ByteBuffer = {
    ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length)
  }

  /** Returns the [[ByteBuffer]] representation of the specified hex [[String]].
    *
    * @param hex an hexadecimal representation of a byte array
    * @return the [[ByteBuffer]] representation of `hex`
    */
  def byteBuffer(hex: String): ByteBuffer = {
    if (hex == null) null else ByteBufferUtil.hexToBytes(hex)
  }

  /** Returns a [[ByteBuffer]] representing the specified array of [[ByteBuffer]]s.
    *
    * @param bbs an array of byte buffers
    * @return a [[ByteBuffer]] representing `bbs`
    */
  @varargs
  def compose(bbs: ByteBuffer*): ByteBuffer = {
    val totalLength = (2 /: bbs.map(_.remaining)) (_ + _ + 2)
    val out = ByteBuffer.allocate(totalLength)
    writeShortLength(out, bbs.length)
    for (bb <- bbs) {
      writeShortLength(out, bb.remaining)
      out.put(bb.duplicate)
    }
    out.flip
    out
  }

  /** Returns the components of the specified [[ByteBuffer]] created with [[compose()]].
    *
    * @param bb a byte buffer created with [[compose()]]
    * @return the components of `bb`
    */
  def decompose(bb: ByteBuffer): Array[ByteBuffer] = {
    val duplicate = bb.duplicate
    val numComponents = readShortLength(duplicate)
    (1 to numComponents).map(_ => {
      val componentLength = readShortLength(duplicate)
      ByteBufferUtil.readBytes(duplicate, componentLength)
    }).toArray
  }
}
