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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Halt {
  private static final Logger log = LoggerFactory.getLogger(Halt.class);

  public static void halt(final String msg) {
    Runnable logRunnable = createLogRunnable(msg);
    halt(0, logRunnable);
  }

  public static void halt(final String msg, int status) {
    Runnable logRunnable = createLogRunnable(msg);
    halt(status, logRunnable);
  }

  private static Runnable createLogRunnable(final String msg) {
    return () -> log.error("FATAL {}", msg);
  }

  public static void halt(final int status, Runnable runnable) {
    executeRunnable(runnable);
    initiateHalt(status);
  }

  private static void executeRunnable(Runnable runnable) {
    if (runnable != null) {
      runnable.run();
    }
  }

  private static void initiateHalt(int status) {
    try {
      Threads.createThread("Halt Thread", () -> {
        sleepUninterruptibly(100, MILLISECONDS);
        Runtime.getRuntime().halt(status);
      }).start();
      Runtime.getRuntime().halt(status);
    } finally {
      Runtime.getRuntime().halt(-1);
    }
  }
}
