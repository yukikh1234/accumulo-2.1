
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

import java.util.Iterator;

public class PeekingIterator<E> implements Iterator<E> {

  private boolean isInitialized;
  private Iterator<E> source;
  private E nextElement;

  public PeekingIterator(Iterator<E> source) {
    initializeWithSource(source);
  }

  public PeekingIterator() {
    this.isInitialized = false;
  }

  public PeekingIterator<E> initialize(Iterator<E> source) {
    initializeWithSource(source);
    return this;
  }

  private void initializeWithSource(Iterator<E> source) {
    if (source == null) {
      throw new IllegalArgumentException("Source iterator cannot be null");
    }
    this.source = source;
    this.nextElement = source.hasNext() ? source.next() : null;
    this.isInitialized = true;
  }

  public E peek() {
    ensureInitialized();
    return nextElement;
  }

  @Override
  public E next() {
    ensureInitialized();
    E currentElement = nextElement;
    nextElement = source.hasNext() ? source.next() : null;
    return currentElement;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNext() {
    ensureInitialized();
    return nextElement != null;
  }

  private void ensureInitialized() {
    if (!isInitialized) {
      throw new IllegalStateException("Iterator has not yet been initialized");
    }
  }
}
