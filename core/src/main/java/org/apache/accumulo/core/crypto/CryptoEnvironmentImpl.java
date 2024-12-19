
package org.apache.accumulo.core.crypto;

import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * @since 2.0
 */
public class CryptoEnvironmentImpl implements CryptoEnvironment {

  private final Scope scope;
  private final TableId tableId;
  private final byte[] decryptionParams;

  /**
   * Construct the crypto environment. The decryptionParams can be null.
   */
  public CryptoEnvironmentImpl(Scope scope, @Nullable TableId tableId,
      @Nullable byte[] decryptionParams) {
    this.scope = Objects.requireNonNull(scope, "Scope cannot be null");
    this.tableId = tableId;
    this.decryptionParams = decryptionParams;
  }

  public CryptoEnvironmentImpl(Scope scope) {
    this(scope, null, null);
  }

  @Override
  public Scope getScope() {
    return scope;
  }

  @Override
  public Optional<TableId> getTableId() {
    return Optional.ofNullable(tableId);
  }

  @Override
  public Optional<byte[]> getDecryptionParams() {
    return Optional.ofNullable(decryptionParams);
  }

  @Override
  public String toString() {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("Scope: ").append(scope).append(", TableId: ").append(tableId)
        .append(", DecryptParams.length: ");
    if (decryptionParams == null) {
      strBuilder.append(0);
    } else {
      strBuilder.append(decryptionParams.length);
    }
    return strBuilder.toString();
  }
}
