
package org.apache.accumulo.core.util;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Serializes and deserializes Thrift messages to and from byte arrays. This class is not
 * thread-safe, external synchronization is necessary if it is used concurrently.
 */
public class ThriftMessageUtil {

  private final int initialCapacity;
  private final TMemoryInputTransport inputTransport;
  private final TCompactProtocol inputProtocol;

  public ThriftMessageUtil() throws IOException {
    this.initialCapacity = 64;
    try {
      this.inputTransport = new TMemoryInputTransport();
    } catch (TTransportException e) {
      throw new IOException(e);
    }
    this.inputProtocol = new TCompactProtocol(inputTransport);
  }

  /**
   * Convert the {@code msg} to a byte array representation
   *
   * @param msg The message to serialize
   * @return The serialized message
   * @throws IOException When serialization fails
   */
  public ByteBuffer serialize(TBase<?,?> msg) throws IOException {
    requireNonNull(msg);
    try (TMemoryBuffer transport = new TMemoryBuffer(initialCapacity)) {
      TProtocol protocol = new TCompactProtocol(transport);
      msg.write(protocol);
      return ByteBuffer.wrap(transport.getArray(), 0, transport.length());
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  /**
   * Assumes the entire contents of the byte array compose the serialized {@code instance}
   *
   * @see #deserialize(byte[], int, int, TBase)
   */
  public <T extends TBase<?,?>> T deserialize(byte[] serialized, T instance) throws IOException {
    return deserialize(serialized, 0, serialized.length, instance);
  }

  /**
   * Deserializes a message into the provided {@code instance} from {@code serialized}
   *
   * @param serialized The serialized representation of the object
   * @param instance An instance of the object to reconstitute
   * @return The reconstituted instance provided
   * @throws IOException When deserialization fails
   */
  public <T extends TBase<?,?>> T deserialize(byte[] serialized, int offset, int length, T instance)
      throws IOException {
    requireNonNull(instance);
    resetTransport(serialized, offset, length);
    readInstance(instance);
    return instance;
  }

  private void resetTransport(byte[] serialized, int offset, int length) {
    inputTransport.reset(serialized, offset, length);
  }

  private <T extends TBase<?,?>> void readInstance(T instance) throws IOException {
    try {
      instance.read(inputProtocol);
    } catch (TException e) {
      throw new IOException(e);
    }
  }
}
