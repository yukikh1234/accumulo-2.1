
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.io.Text;

public class ByteBufferUtil {

  public static byte[] toBytes(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    return buffer.hasArray() ? Arrays.copyOfRange(buffer.array(),
        buffer.position() + buffer.arrayOffset(), buffer.limit() + buffer.arrayOffset())
        : toBytesFromBuffer(buffer);
  }

  private static byte[] toBytesFromBuffer(ByteBuffer buffer) {
    byte[] data = new byte[buffer.remaining()];
    buffer.duplicate().get(data);
    return data;
  }

  public static List<ByteBuffer> toByteBuffers(Collection<byte[]> bytesList) {
    if (bytesList == null) {
      return null;
    }
    List<ByteBuffer> result = new ArrayList<>(bytesList.size());
    for (byte[] bytes : bytesList) {
      result.add(ByteBuffer.wrap(bytes));
    }
    return result;
  }

  public static List<byte[]> toBytesList(Collection<ByteBuffer> bytesList) {
    if (bytesList == null) {
      return null;
    }
    List<byte[]> result = new ArrayList<>(bytesList.size());
    for (ByteBuffer bytes : bytesList) {
      result.add(toBytes(bytes));
    }
    return result;
  }

  public static Set<String> toStringSet(Collection<ByteBuffer> bytesList) {
    if (bytesList == null) {
      return null;
    }
    Set<String> result = new HashSet<>(bytesList.size());
    for (ByteBuffer bytes : bytesList) {
      result.add(toString(bytes));
    }
    return result;
  }

  public static Text toText(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    return byteBuffer.hasArray() ? toTextFromArray(byteBuffer) : new Text(toBytes(byteBuffer));
  }

  private static Text toTextFromArray(ByteBuffer byteBuffer) {
    Text result = new Text();
    result.set(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(),
        byteBuffer.remaining());
    return result;
  }

  public static String toString(ByteBuffer bytes) {
    return bytes.hasArray() ? new String(bytes.array(), bytes.arrayOffset() + bytes.position(),
        bytes.remaining(), UTF_8) : new String(toBytes(bytes), UTF_8);
  }

  public static TableId toTableId(ByteBuffer bytes) {
    return TableId.of(toString(bytes));
  }

  public static ByteBuffer toByteBuffers(ByteSequence bs) {
    if (bs == null) {
      return null;
    }
    return bs.isBackedByArray() ? ByteBuffer.wrap(bs.getBackingArray(), bs.offset(), bs.length())
        : ByteBuffer.wrap(bs.toArray());
  }

  public static void write(DataOutput out, ByteBuffer buffer) throws IOException {
    if (buffer.hasArray()) {
      out.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
    } else {
      out.write(toBytes(buffer));
    }
  }

  public static ByteArrayInputStream toByteArrayInputStream(ByteBuffer buffer) {
    return buffer.hasArray() ? new ByteArrayInputStream(buffer.array(),
        buffer.arrayOffset() + buffer.position(), buffer.remaining())
        : new ByteArrayInputStream(toBytes(buffer));
  }
}
