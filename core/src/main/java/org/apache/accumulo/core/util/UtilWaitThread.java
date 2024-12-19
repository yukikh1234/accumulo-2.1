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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilWaitThread {
  private static final Logger log = LoggerFactory.getLogger(UtilWaitThread.class);

  public static void sleep(long millis) {
    sleepWithLogging(millis);
  }

  private static void sleepWithLogging(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
    }
  }

  public static void sleepUninterruptibly(long sleepFor, TimeUnit unit) {
    boolean interrupted = false;
    long remainingNanos = unit.toNanos(sleepFor);
    long end = System.nanoTime() + remainingNanos;
    while (true) {
      try {
        sleepForRemainingNanos(remainingNanos);
        return;
      } catch (InterruptedException e) {
        interrupted = true;
        remainingNanos = end - System.nanoTime();
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private static void sleepForRemainingNanos(long remainingNanos) throws InterruptedException {
    NANOSECONDS.sleep(remainingNanos);
  }
}
