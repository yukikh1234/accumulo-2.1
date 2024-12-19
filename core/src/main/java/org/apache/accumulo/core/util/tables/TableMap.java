
package org.apache.accumulo.core.util.tables;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class TableMap {
  private static final Logger log = LoggerFactory.getLogger(TableMap.class);

  private final Map<String,TableId> tableNameToIdMap;
  private final Map<TableId,String> tableIdToNameMap;

  private final ZooCache zooCache;
  private final long updateCount;

  public TableMap(ClientContext context) {
    this.zooCache = context.getZooCache();
    this.updateCount = zooCache.getUpdateCount();

    List<String> tableIds = zooCache.getChildren(context.getZooKeeperRoot() + Constants.ZTABLES);
    Map<NamespaceId,String> namespaceIdToNameMap = new HashMap<>();

    this.tableNameToIdMap = buildTableNameToIdMap(context, tableIds, namespaceIdToNameMap);
    this.tableIdToNameMap = buildTableIdToNameMap(context, tableIds, namespaceIdToNameMap);
  }

  private Map<String,TableId> buildTableNameToIdMap(ClientContext context, List<String> tableIds,
      Map<NamespaceId,String> namespaceIdToNameMap) {
    final var tableNameToIdBuilder = ImmutableMap.<String,TableId>builder();
    StringBuilder zPathBuilder = new StringBuilder();
    zPathBuilder.append(context.getZooKeeperRoot()).append(Constants.ZTABLES).append("/");
    int prefixLength = zPathBuilder.length();

    for (String tableIdStr : tableIds) {
      zPathBuilder.setLength(prefixLength);
      zPathBuilder.append(tableIdStr).append(Constants.ZTABLE_NAME);
      byte[] tableName = zooCache.get(zPathBuilder.toString());

      String namespaceName =
          getNamespaceName(context, tableIdStr, namespaceIdToNameMap, zPathBuilder, prefixLength);
      if (tableName != null && namespaceName != null) {
        String tableNameStr = TableNameUtil.qualified(new String(tableName, UTF_8), namespaceName);
        TableId tableId = TableId.of(tableIdStr);
        tableNameToIdBuilder.put(tableNameStr, tableId);
      }
    }
    return tableNameToIdBuilder.build();
  }

  private Map<TableId,String> buildTableIdToNameMap(ClientContext context, List<String> tableIds,
      Map<NamespaceId,String> namespaceIdToNameMap) {
    final var tableIdToNameBuilder = ImmutableMap.<TableId,String>builder();
    StringBuilder zPathBuilder = new StringBuilder();
    zPathBuilder.append(context.getZooKeeperRoot()).append(Constants.ZTABLES).append("/");
    int prefixLength = zPathBuilder.length();

    for (String tableIdStr : tableIds) {
      zPathBuilder.setLength(prefixLength);
      zPathBuilder.append(tableIdStr).append(Constants.ZTABLE_NAME);
      byte[] tableName = zooCache.get(zPathBuilder.toString());

      String namespaceName =
          getNamespaceName(context, tableIdStr, namespaceIdToNameMap, zPathBuilder, prefixLength);
      if (tableName != null && namespaceName != null) {
        String tableNameStr = TableNameUtil.qualified(new String(tableName, UTF_8), namespaceName);
        TableId tableId = TableId.of(tableIdStr);
        tableIdToNameBuilder.put(tableId, tableNameStr);
      }
    }
    return tableIdToNameBuilder.build();
  }

  private String getNamespaceName(ClientContext context, String tableIdStr,
      Map<NamespaceId,String> namespaceIdToNameMap, StringBuilder zPathBuilder, int prefixLength) {
    zPathBuilder.setLength(prefixLength);
    zPathBuilder.append(tableIdStr).append(Constants.ZTABLE_NAMESPACE);
    byte[] nId = zooCache.get(zPathBuilder.toString());

    if (nId == null) {
      return null;
    }

    NamespaceId namespaceId = NamespaceId.of(new String(nId, UTF_8));
    if (namespaceId.equals(Namespace.DEFAULT.id())) {
      return Namespace.DEFAULT.name();
    }

    try {
      String namespaceName = namespaceIdToNameMap.get(namespaceId);
      if (namespaceName == null) {
        namespaceName = Namespaces.getNamespaceName(context, namespaceId);
        namespaceIdToNameMap.put(namespaceId, namespaceName);
      }
      return namespaceName;
    } catch (NamespaceNotFoundException e) {
      log.error("Table (" + tableIdStr + ") contains reference to namespace (" + namespaceId
          + ") that doesn't exist", e);
      return null;
    }
  }

  public Map<String,TableId> getNameToIdMap() {
    return tableNameToIdMap;
  }

  public Map<TableId,String> getIdtoNameMap() {
    return tableIdToNameMap;
  }

  public boolean isCurrent(ZooCache zc) {
    return this.zooCache == zc && this.updateCount == zc.getUpdateCount();
  }
}
