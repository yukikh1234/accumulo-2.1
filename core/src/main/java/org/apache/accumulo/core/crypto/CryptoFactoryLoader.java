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

package org.apache.accumulo.core.crypto;

import static org.apache.accumulo.core.crypto.CryptoFactoryLoader.ClassloaderType.ACCUMULO;
import static org.apache.accumulo.core.crypto.CryptoFactoryLoader.ClassloaderType.JAVA;
import static org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope.TABLE;

import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.spi.crypto.GenericCryptoServiceFactory;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoFactoryLoader {
  private static final Logger log = LoggerFactory.getLogger(CryptoFactoryLoader.class);
  private static final CryptoServiceFactory NO_CRYPTO_FACTORY = new NoCryptoServiceFactory();

  enum ClassloaderType {
    ACCUMULO, JAVA
  }

  public static CryptoServiceFactory newInstance(AccumuloConfiguration conf) {
    return loadCryptoFactory(ACCUMULO, conf.get(Property.INSTANCE_CRYPTO_FACTORY));
  }

  public static CryptoService getServiceForServer(AccumuloConfiguration conf) {
    var env = new CryptoEnvironmentImpl(TABLE, null, null);
    var factory = newInstance(conf);
    return factory.getService(env, conf.getAllCryptoProperties());
  }

  public static CryptoService getServiceForClient(CryptoEnvironment.Scope scope,
      Map<String,String> properties) {
    var factory = loadCryptoFactory(JAVA, GenericCryptoServiceFactory.class.getName());
    var env = new CryptoEnvironmentImpl(scope, null, null);
    return factory.getService(env, properties);
  }

  public static CryptoService getServiceForClientWithTable(Map<String,String> systemConfig,
      Map<String,String> tableProps, TableId tableId) {
    String clazzName = getFactoryClassName(systemConfig);
    if (clazzName == null) {
      return NoCryptoServiceFactory.NONE;
    }

    var env = new CryptoEnvironmentImpl(TABLE, tableId, null);
    var factory = loadCryptoFactory(JAVA, clazzName);
    return factory.getService(env, tableProps);
  }

  private static String getFactoryClassName(Map<String,String> config) {
    String clazzName = config.get(Property.INSTANCE_CRYPTO_FACTORY.getKey());
    return (clazzName == null || clazzName.trim().isEmpty()) ? null : clazzName;
  }

  private static CryptoServiceFactory loadCryptoFactory(ClassloaderType ct, String clazzName) {
    log.debug("Creating new crypto factory class {}", clazzName);
    if (ct == ACCUMULO) {
      return loadAccumuloCryptoFactory(clazzName);
    } else if (ct == JAVA) {
      return loadJavaCryptoFactory(clazzName);
    } else {
      throw new IllegalArgumentException();
    }
  }

  private static CryptoServiceFactory loadAccumuloCryptoFactory(String clazzName) {
    return ConfigurationTypeHelper.getClassInstance(null, clazzName, CryptoServiceFactory.class,
        new NoCryptoServiceFactory());
  }

  private static CryptoServiceFactory loadJavaCryptoFactory(String clazzName) {
    if (clazzName == null || clazzName.trim().isEmpty()) {
      return NO_CRYPTO_FACTORY;
    }
    try {
      return CryptoFactoryLoader.class.getClassLoader().loadClass(clazzName)
          .asSubclass(CryptoServiceFactory.class).getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
