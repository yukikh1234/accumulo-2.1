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
package org.apache.accumulo.shell.commands;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.io.Text;

public class MaxRowCommand extends ScanCommand {

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    final String tableName = OptUtil.getTableOpt(cl, shellState);

    @SuppressWarnings("deprecation")
    final org.apache.accumulo.core.util.interpret.ScanInterpreter interpreter =
        getInterpreter(cl, tableName, shellState);

    final Range range = getRange(cl, interpreter);
    final Authorizations auths = getAuths(cl, shellState);
    final Text startRow = range.getStartKey() == null ? null : range.getStartKey().getRow();
    final Text endRow = range.getEndKey() == null ? null : range.getEndKey().getRow();

    try {
      final Text max = shellState.getAccumuloClient().tableOperations().getMaxRow(tableName, auths,
          startRow, range.isStartKeyInclusive(), endRow, range.isEndKeyInclusive());
      if (max != null) {
        shellState.getWriter().println(max);
      }
    } catch (Exception e) {
      Shell.log.debug("Could not get shell state.", e);
    }

    return 0;
  }

  @Override
  public String description() {
    return "finds the max row in a table within a given range";
  }
}
