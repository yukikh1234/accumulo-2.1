
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

import org.apache.accumulo.start.Main;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class Version implements KeywordExecutable {

  @Override
  public String keyword() {
    return "version";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.CORE;
  }

  @Override
  public String description() {
    return "Prints Accumulo version";
  }

  @Override
  public void execute(final String[] args) throws Exception {
    Class<?> versionClass = loadVersionClass();
    printVersion(versionClass);
  }

  private Class<?> loadVersionClass() throws ClassNotFoundException {
    return Main.getClassLoader().loadClass("org.apache.accumulo.core.Constants");
  }

  private void printVersion(Class<?> versionClass) throws Exception {
    System.out.println(versionClass.getField("VERSION").get(null));
  }
}
