
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

import java.text.DecimalFormat;

public class NumUtil {

  private static final String[] QUANTITY_SUFFIX = {"", "K", "M", "B", "T", "e15", "e18", "e21"};
  private static final String[] SIZE_SUFFIX = {"", "K", "M", "G", "T", "P", "E", "Z"};

  private static final DecimalFormat df = new DecimalFormat("#,###,##0");
  private static final DecimalFormat df_mantissa = new DecimalFormat("#,###,##0.00");

  public static String bigNumberForSize(long big) {
    return calculateBigNumber(big, SIZE_SUFFIX, 1024);
  }

  public static String bigNumberForQuantity(long big) {
    return calculateBigNumber(big, QUANTITY_SUFFIX, 1000);
  }

  public static String bigNumberForQuantity(double big) {
    return calculateBigNumber(big, QUANTITY_SUFFIX, 1000.0);
  }

  private static String calculateBigNumber(long big, String[] suffixes, long base) {
    if (big < base) {
      return df.format(big) + suffixes[0];
    }
    int exp = (int) (Math.log(big) / Math.log(base));
    double val = big / Math.pow(base, exp);
    return df_mantissa.format(val) + suffixes[exp];
  }

  private static String calculateBigNumber(double big, String[] suffixes, double base) {
    if (big < base) {
      return df_mantissa.format(big) + suffixes[0];
    }
    int exp = (int) (Math.log(big) / Math.log(base));
    double val = big / Math.pow(base, exp);
    return df_mantissa.format(val) + suffixes[exp];
  }
}
