
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
package org.apache.accumulo.core.util.compaction;

import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;

import com.google.common.base.Preconditions;

public class CompactionExecutorIdImpl extends CompactionExecutorId {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new CompactionExecutorIdImpl with the given canonical name.
   *
   * @param canonical the canonical name of the compaction executor
   */
  protected CompactionExecutorIdImpl(String canonical) {
    super(canonical);
  }

  /**
   * Checks if the current executor ID is external.
   *
   * @return true if the ID starts with "e.", false otherwise
   */
  public boolean isExternalId() {
    return canonical().startsWith("e.");
  }

  /**
   * Retrieves the external name of the executor. Preconditions: The ID must be external (i.e.,
   * {@link #isExternalId()} returns true).
   *
   * @return the external name of the executor
   * @throws IllegalStateException if the ID is not external
   */
  public String getExternalName() {
    Preconditions.checkState(isExternalId(), "The ID is not external.");
    return canonical().substring(2); // "e.".length() is 2
  }

  /**
   * Creates an internal compaction executor ID.
   *
   * @param csid the compaction service ID
   * @param executorName the name of the executor
   * @return a new CompactionExecutorId representing an internal executor
   */
  public static CompactionExecutorId internalId(CompactionServiceId csid, String executorName) {
    return new CompactionExecutorIdImpl("i." + csid + "." + executorName);
  }

  /**
   * Creates an external compaction executor ID.
   *
   * @param executorName the name of the executor
   * @return a new CompactionExecutorId representing an external executor
   */
  public static CompactionExecutorId externalId(String executorName) {
    return new CompactionExecutorIdImpl("e." + executorName);
  }
}
