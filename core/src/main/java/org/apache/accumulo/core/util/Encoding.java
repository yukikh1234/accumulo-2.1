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

import java.util.Base64;

public class Encoding {

  /**
   * Encodes the provided byte array into a Base64 string suitable for use in file names. The method
   * uses URL-safe encoding and trims trailing '=' characters for better readability.
   *
   * @param data the byte array to encode
   * @return a Base64 encoded string with '=' characters removed
   */
  public static String encodeAsBase64FileName(byte[] data) {
    // Encode the input data using Base64 URL-safe encoding
    String encodedRow = Base64.getUrlEncoder().withoutPadding().encodeToString(data);

    return encodedRow;
  }

  /**
   * Decodes a Base64-encoded file name string back into a byte array.
   *
   * @param node the Base64 encoded string
   * @return the decoded byte array
   */
  public static byte[] decodeBase64FileName(String node) {
    // Decode the Base64 URL-safe encoded string back to byte array
    return Base64.getUrlDecoder().decode(node);
  }
}
