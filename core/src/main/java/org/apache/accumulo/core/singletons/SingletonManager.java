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

package org.apache.accumulo.core.singletons;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class SingletonManager {

  private static final Logger log = LoggerFactory.getLogger(SingletonManager.class);

  public enum Mode {
    CLIENT, SERVER, CONNECTOR, CLOSED
  }

  private static long reservations;
  private static Mode mode;
  private static boolean enabled;
  private static boolean transitionedFromClientToConnector;
  private static List<SingletonService> services;

  @VisibleForTesting
  static void reset() {
    reservations = 0;
    mode = Mode.CLIENT;
    enabled = true;
    transitionedFromClientToConnector = false;
    services = new ArrayList<>();
  }

  static {
    reset();
  }

  private static void manageService(SingletonService service, boolean enable) {
    try {
      if (enable) {
        service.enable();
      } else {
        service.disable();
      }
    } catch (RuntimeException e) {
      log.error("Failed to " + (enable ? "enable" : "disable") + " singleton service", e);
    }
  }

  public static synchronized void register(SingletonService service) {
    manageService(service, enabled);
    services.add(service);
  }

  public static synchronized SingletonReservation getClientReservation() {
    Preconditions.checkState(reservations >= 0);
    reservations++;
    transition();
    return new SingletonReservation();
  }

  static synchronized void releaseReservation() {
    Preconditions.checkState(reservations > 0);
    reservations--;
    transition();
  }

  @VisibleForTesting
  public static long getReservationCount() {
    return reservations;
  }

  public static synchronized void setMode(Mode newMode) {
    if (mode == newMode) {
      return;
    }
    if (mode == Mode.CLOSED) {
      throw new IllegalStateException("Cannot leave closed mode once entered");
    }
    if (mode == Mode.CLIENT && newMode == Mode.CONNECTOR) {
      handleClientToConnectorTransition();
    }
    if (mode != Mode.SERVER || newMode == Mode.CLOSED) {
      mode = newMode;
    }
    transition();
  }

  private static void handleClientToConnectorTransition() {
    if (transitionedFromClientToConnector) {
      throw new IllegalStateException("Can only transition from CLIENT to CONNECTOR once.");
    }
    transitionedFromClientToConnector = true;
  }

  @VisibleForTesting
  public static synchronized Mode getMode() {
    return mode;
  }

  private static void transition() {
    boolean shouldEnable = (mode == Mode.CONNECTOR || mode == Mode.SERVER
        || (mode == Mode.CLIENT && reservations > 0));
    if (enabled != shouldEnable) {
      services.forEach(service -> manageService(service, shouldEnable));
      enabled = shouldEnable;
    }
  }
}
