
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

import java.net.UnknownHostException;
import java.security.Security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddressUtil {

  private static final Logger log = LoggerFactory.getLogger(AddressUtil.class);

  private static final int DEFAULT_NEGATIVE_TTL = 10;

  public static HostAndPort parseAddress(String address, boolean ignoreMissingPort)
      throws NumberFormatException {
    address = address.replace('+', ':');
    HostAndPort hap = HostAndPort.fromString(address);
    if (!ignoreMissingPort && !hap.hasPort()) {
      throw new IllegalArgumentException(
          "Address was expected to contain port. address=" + address);
    }
    return hap;
  }

  public static HostAndPort parseAddress(String address, int defaultPort) {
    return parseAddress(address, true).withDefaultPort(defaultPort);
  }

  public static int getAddressCacheNegativeTtl(UnknownHostException originalException) {
    int negativeTtl = getNegativeTtlFromSecurityProperty();
    validateNegativeTtl(negativeTtl, originalException);
    return negativeTtl < 0 ? DEFAULT_NEGATIVE_TTL : negativeTtl;
  }

  private static int getNegativeTtlFromSecurityProperty() {
    try {
      return Integer.parseInt(Security.getProperty("networkaddress.cache.negative.ttl"));
    } catch (NumberFormatException e) {
      log.warn("Failed to get JVM negative DNS response cache TTL due to format problem. "
          + "Falling back to default based on Oracle JVM 1.4+ (10s)", e);
    } catch (SecurityException e) {
      log.warn("Failed to get JVM negative DNS response cache TTL due to security manager. "
          + "Falling back to default based on Oracle JVM 1.4+ (10s)", e);
    }
    return DEFAULT_NEGATIVE_TTL;
  }

  private static void validateNegativeTtl(int negativeTtl, UnknownHostException originalException) {
    if (negativeTtl == -1) {
      log.error(
          "JVM negative DNS response cache TTL is set to 'forever' and host lookup failed. "
              + "TTL can be changed with security property "
              + "'networkaddress.cache.negative.ttl', see java.net.InetAddress.",
          originalException);
      throw new IllegalArgumentException(originalException);
    } else if (negativeTtl < 0) {
      log.warn("JVM specified negative DNS response cache TTL was negative (and not 'forever'). "
          + "Falling back to default based on Oracle JVM 1.4+ (10s)");
    }
  }
}
