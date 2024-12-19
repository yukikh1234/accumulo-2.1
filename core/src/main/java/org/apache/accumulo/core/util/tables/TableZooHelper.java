
package org.apache.accumulo.core.util.tables;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.core.util.Validators.EXISTING_TABLE_NAME;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class TableZooHelper implements AutoCloseable {

  private final ClientContext context;
  private final Cache<TableZooHelper,TableMap> instanceToMapCache =
      CacheBuilder.newBuilder().expireAfterAccess(10, MINUTES).build();

  public TableZooHelper(ClientContext context) {
    this.context = Objects.requireNonNull(context);
  }

  public TableId getTableId(String tableName) throws TableNotFoundException {
    if (MetadataTable.NAME.equals(tableName)) {
      return MetadataTable.ID;
    }
    if (RootTable.NAME.equals(tableName)) {
      return RootTable.ID;
    }
    return fetchTableId(tableName);
  }

  private TableId fetchTableId(String tableName) throws TableNotFoundException {
    try {
      return _getTableIdDetectNamespaceNotFound(EXISTING_TABLE_NAME.validate(tableName));
    } catch (NamespaceNotFoundException e) {
      throw new TableNotFoundException(tableName, e);
    }
  }

  public TableId _getTableIdDetectNamespaceNotFound(String tableName)
      throws NamespaceNotFoundException, TableNotFoundException {
    TableId tableId = getTableMap().getNameToIdMap().get(tableName);
    if (tableId == null) {
      clearTableListCache();
      tableId = getTableMap().getNameToIdMap().get(tableName);
      if (tableId == null) {
        handleNamespaceNotFound(tableName);
      }
    }
    return tableId;
  }

  private void handleNamespaceNotFound(String tableName)
      throws NamespaceNotFoundException, TableNotFoundException {
    String namespace = TableNameUtil.qualify(tableName).getFirst();
    if (!Namespaces.getNameToIdMap(context).containsKey(namespace)) {
      throw new NamespaceNotFoundException(null, namespace, null);
    }
    throw new TableNotFoundException(null, tableName, null);
  }

  public String getTableName(TableId tableId) throws TableNotFoundException {
    if (MetadataTable.ID.equals(tableId)) {
      return MetadataTable.NAME;
    }
    if (RootTable.ID.equals(tableId)) {
      return RootTable.NAME;
    }
    String tableName = getTableMap().getIdtoNameMap().get(tableId);
    if (tableName == null) {
      throw new TableNotFoundException(tableId.canonical(), null, null);
    }
    return tableName;
  }

  public TableMap getTableMap() {
    final ZooCache zc = context.getZooCache();
    TableMap map = getCachedTableMap();
    if (!map.isCurrent(zc)) {
      instanceToMapCache.invalidateAll();
      map = getCachedTableMap();
    }
    return map;
  }

  private TableMap getCachedTableMap() {
    try {
      return instanceToMapCache.get(this, () -> new TableMap(context));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean tableNodeExists(TableId tableId) {
    ZooCache zc = context.getZooCache();
    List<String> tableIds = zc.getChildren(context.getZooKeeperRoot() + Constants.ZTABLES);
    return tableIds.contains(tableId.canonical());
  }

  public void clearTableListCache() {
    context.getZooCache().clear(context.getZooKeeperRoot() + Constants.ZTABLES);
    context.getZooCache().clear(context.getZooKeeperRoot() + Constants.ZNAMESPACES);
    instanceToMapCache.invalidateAll();
  }

  public String getPrintableTableInfoFromId(TableId tableId) {
    try {
      return _printableTableInfo(getTableName(tableId), tableId);
    } catch (TableNotFoundException e) {
      return _printableTableInfo(null, tableId);
    }
  }

  public String getPrintableTableInfoFromName(String tableName) {
    try {
      return _printableTableInfo(tableName, getTableId(tableName));
    } catch (TableNotFoundException e) {
      return _printableTableInfo(tableName, null);
    }
  }

  private String _printableTableInfo(String tableName, TableId tableId) {
    return String.format("%s(ID:%s)", tableName == null ? "?" : tableName,
        tableId == null ? "?" : tableId.canonical());
  }

  public TableState getTableState(TableId tableId, boolean clearCachedState) {
    String statePath = context.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId.canonical()
        + Constants.ZTABLE_STATE;
    if (clearCachedState) {
      context.getZooCache().clear(context.getZooKeeperRoot() + statePath);
      instanceToMapCache.invalidateAll();
    }
    return fetchTableState(statePath);
  }

  private TableState fetchTableState(String statePath) {
    ZooCache zc = context.getZooCache();
    byte[] state = zc.get(statePath);
    if (state == null) {
      return TableState.UNKNOWN;
    }
    return TableState.valueOf(new String(state, UTF_8));
  }

  public NamespaceId getNamespaceId(TableId tableId) throws TableNotFoundException {
    checkArgument(context != null, "instance is null");
    checkArgument(tableId != null, "tableId is null");

    if (MetadataTable.ID.equals(tableId) || RootTable.ID.equals(tableId)) {
      return Namespace.ACCUMULO.id();
    }

    return fetchNamespaceId(tableId);
  }

  private NamespaceId fetchNamespaceId(TableId tableId) throws TableNotFoundException {
    ZooCache zc = context.getZooCache();
    byte[] n = zc.get(context.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId
        + Constants.ZTABLE_NAMESPACE);
    if (n == null) {
      throw new TableNotFoundException(tableId.canonical(), null, null);
    }
    return NamespaceId.of(new String(n, UTF_8));
  }

  @Override
  public void close() {
    instanceToMapCache.invalidateAll();
  }
}
