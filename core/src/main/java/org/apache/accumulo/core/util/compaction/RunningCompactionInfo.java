
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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.TreeMap;

import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunningCompactionInfo {
  private static final Logger log = LoggerFactory.getLogger(RunningCompactionInfo.class);

  // DO NOT CHANGE Variable names - they map to JSON keys in the Monitor
  public final String server;
  public final String queueName;
  public final String ecid;
  public final String kind;
  public final String tableId;
  public final int numFiles;
  public final float progress;
  public final long duration;
  public final String status;
  public final long lastUpdate;

  /**
   * Info parsed about the external running compaction. Calculate the progress, which is defined as
   * the percentage of bytesRead / bytesToBeCompacted of the last update.
   */
  public RunningCompactionInfo(TExternalCompaction ec) {
    requireNonNull(ec, "Thrift external compaction is null.");

    var updates = parseUpdates(ec);
    var job = requireNonNull(ec.getJob(), "Thrift external compaction job is null");

    server = ec.getCompactor();
    queueName = ec.getQueueName();
    ecid = job.getExternalCompactionId();
    kind = job.getKind().name();
    tableId = KeyExtent.fromThrift(job.getExtent()).tableId().canonical();
    numFiles = job.getFiles().size();

    long nowMillis = System.currentTimeMillis();
    long updateMillis;
    TCompactionStatusUpdate last;

    var lastEntry = getLastUpdateEntry(updates);

    if (lastEntry != null) {
      last = lastEntry.getValue();
      updateMillis = lastEntry.getKey();
      duration = last.getCompactionAgeNanos();
    } else {
      log.debug("No updates found for {}", ecid);
      lastUpdate = 1;
      progress = 0f;
      status = "na";
      duration = 0;
      return;
    }

    checkDuration(duration, ecid);

    lastUpdate = nowMillis - updateMillis;
    long sinceLastUpdateSeconds = MILLISECONDS.toSeconds(lastUpdate);
    log.debug("Time since Last update {} - {} = {} seconds", nowMillis, updateMillis,
        sinceLastUpdateSeconds);

    progress = calculateProgress(last);
    status = determineStatus(updates, last);
    log.debug("Parsed running compaction {} for {} with progress = {}%", status, ecid, progress);
    if (sinceLastUpdateSeconds > 30) {
      log.debug("Compaction hasn't progressed from {} in {} seconds.", progress,
          sinceLastUpdateSeconds);
    }
  }

  private TreeMap<Long,TCompactionStatusUpdate> parseUpdates(TExternalCompaction ec) {
    return new TreeMap<>(
        requireNonNull(ec.getUpdates(), "Missing Thrift external compaction updates"));
  }

  private java.util.Map.Entry<Long,TCompactionStatusUpdate>
      getLastUpdateEntry(TreeMap<Long,TCompactionStatusUpdate> updates) {
    return updates.lastEntry();
  }

  private void checkDuration(long duration, String ecid) {
    long durationMinutes = NANOSECONDS.toMinutes(duration);
    if (durationMinutes > 15) {
      log.warn("Compaction {} has been running for {} minutes", ecid, durationMinutes);
    }
  }

  private float calculateProgress(TCompactionStatusUpdate last) {
    var total = last.getEntriesToBeCompacted();
    return total > 0 ? (last.getEntriesRead() / (float) total) * 100 : 0f;
  }

  private String determineStatus(TreeMap<Long,TCompactionStatusUpdate> updates,
      TCompactionStatusUpdate last) {
    return updates.isEmpty() ? "na" : last.state.name();
  }

  @Override
  public String toString() {
    return ecid + ": " + status + " progress: " + progress;
  }
}
