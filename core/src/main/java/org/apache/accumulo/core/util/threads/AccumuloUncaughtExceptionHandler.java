
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
package org.apache.accumulo.core.util.threads;

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UncaughtExceptionHandler that logs all Exceptions and Errors thrown from a Thread. If an Error is
 * thrown, halt the JVM.
 *
 */
class AccumuloUncaughtExceptionHandler implements UncaughtExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloUncaughtExceptionHandler.class);

  private static boolean isError(Throwable t) {
    return checkError(t, 0);
  }

  private static boolean checkError(Throwable t, int depth) {
    if (depth > 32) {
      // Recursion too deep, assume error to prevent stack overflow.
      return true;
    }
    while (t != null) {
      if (t instanceof Error) {
        return true;
      }
      for (Throwable suppressed : t.getSuppressed()) {
        if (checkError(suppressed, depth + 1)) {
          return true;
        }
      }
      t = t.getCause();
    }
    return false;
  }

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    if (isError(e)) {
      handleCriticalError(t, e);
    } else {
      LOG.error("Caught an Exception in {}. Thread is dead.", t, e);
    }
  }

  private void handleCriticalError(Thread t, Throwable e) {
    try {
      e.printStackTrace();
      System.err.println("Error thrown in thread: " + t + ", halting VM.");
    } catch (Throwable e1) {
      // Handle potential errors in error logging, such as OutOfMemoryError.
    } finally {
      Runtime.getRuntime().halt(-1);
    }
  }
}
