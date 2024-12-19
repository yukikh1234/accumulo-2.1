
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
package org.apache.accumulo.core.file.blockfile.impl;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.logging.LoggingBlockCache;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.apache.accumulo.core.spi.scan.ScanDispatch;

public class ScanCacheProvider implements CacheProvider {

  private final BlockCache indexCache;
  private final BlockCache dataCache;

  public ScanCacheProvider(AccumuloConfiguration tableConfig, ScanDispatch dispatch,
      BlockCache indexCache, BlockCache dataCache) {

    this.indexCache = configureIndexCache(tableConfig, dispatch, indexCache);
    this.dataCache = configureDataCache(tableConfig, dispatch, dataCache);
  }

  private BlockCache configureIndexCache(AccumuloConfiguration tableConfig, ScanDispatch dispatch,
      BlockCache indexCache) {
    var loggingIndexCache = LoggingBlockCache.wrap(CacheType.INDEX, indexCache);
    switch (dispatch.getIndexCacheUsage()) {
      case ENABLED:
        return loggingIndexCache;
      case DISABLED:
        return null;
      case OPPORTUNISTIC:
        return new OpportunisticBlockCache(loggingIndexCache);
      case TABLE:
        return tableConfig.getBoolean(Property.TABLE_INDEXCACHE_ENABLED) ? loggingIndexCache : null;
      default:
        throw new IllegalStateException();
    }
  }

  private BlockCache configureDataCache(AccumuloConfiguration tableConfig, ScanDispatch dispatch,
      BlockCache dataCache) {
    var loggingDataCache = LoggingBlockCache.wrap(CacheType.DATA, dataCache);
    switch (dispatch.getDataCacheUsage()) {
      case ENABLED:
        return loggingDataCache;
      case DISABLED:
        return null;
      case OPPORTUNISTIC:
        return new OpportunisticBlockCache(loggingDataCache);
      case TABLE:
        return tableConfig.getBoolean(Property.TABLE_BLOCKCACHE_ENABLED) ? loggingDataCache : null;
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public BlockCache getDataCache() {
    return dataCache;
  }

  @Override
  public BlockCache getIndexCache() {
    return indexCache;
  }
}
