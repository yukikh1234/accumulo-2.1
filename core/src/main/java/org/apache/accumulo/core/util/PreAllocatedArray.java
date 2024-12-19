
package org.apache.accumulo.core.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * An {@link ArrayList} implementation that represents a type-safe pre-allocated array. This should
 * be used exactly like an array, but helps avoid type-safety issues when mixing arrays with
 * generics. The iterator is unmodifiable.
 */
public class PreAllocatedArray<T> implements Iterable<T> {

  private final ArrayList<T> internal;
  public final int length;

  /**
   * Creates an instance of the given capacity, with all elements initialized to null
   */
  public PreAllocatedArray(final int capacity) {
    this.length = capacity;
    this.internal = new ArrayList<>(Collections.nCopies(capacity, null));
  }

  /**
   * Set the element at the specified index, and return the old value.
   */
  public T set(final int index, final T element) {
    return this.internal.set(index, element);
  }

  /**
   * Get the item stored at the specified index.
   */
  public T get(final int index) {
    return this.internal.get(index);
  }

  @Override
  public Iterator<T> iterator() {
    return Collections.unmodifiableList(this.internal).iterator();
  }
}
