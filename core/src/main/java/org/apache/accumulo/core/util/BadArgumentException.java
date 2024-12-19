
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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.util;

import java.util.regex.PatternSyntaxException;

public final class BadArgumentException extends PatternSyntaxException {
  private static final long serialVersionUID = 1L;

  // Constructor with parameter validation
  public BadArgumentException(String desc, String badarg, int index) {
    super(validateDescription(desc), validateBadArg(badarg), validateIndex(index));
  }

  // Private static methods for validation
  private static String validateDescription(String desc) {
    if (desc == null || desc.isEmpty()) {
      throw new IllegalArgumentException("Description cannot be null or empty");
    }
    return desc;
  }

  private static String validateBadArg(String badarg) {
    if (badarg == null || badarg.isEmpty()) {
      throw new IllegalArgumentException("Bad argument cannot be null or empty");
    }
    return badarg;
  }

  private static int validateIndex(int index) {
    if (index < 0) {
      throw new IllegalArgumentException("Index cannot be negative");
    }
    return index;
  }
}
