
package org.apache.accumulo.core.iteratorsImpl.conf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iteratorsImpl.conf.ColumnUtil.ColFamHashKey;
import org.apache.accumulo.core.iteratorsImpl.conf.ColumnUtil.ColHashKey;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;

public class ColumnToClassMapping<K> {

  private final HashMap<ColFamHashKey,K> objectsCF;
  private final HashMap<ColHashKey,K> objectsCol;

  private final ColHashKey lookupCol = new ColHashKey();
  private final ColFamHashKey lookupCF = new ColFamHashKey();

  public ColumnToClassMapping() {
    objectsCF = new HashMap<>();
    objectsCol = new HashMap<>();
  }

  public ColumnToClassMapping(Map<String,String> objectStrings, Class<? extends K> c)
      throws ReflectiveOperationException, IOException {
    this(objectStrings, c, null);
  }

  public ColumnToClassMapping(Map<String,String> objectStrings, Class<? extends K> c,
      String context) throws ReflectiveOperationException, IOException {
    this();
    initializeMapping(objectStrings, c, context);
  }

  private void initializeMapping(Map<String,String> objectStrings, Class<? extends K> c,
      String context) throws ReflectiveOperationException, IOException {
    for (Entry<String,String> entry : objectStrings.entrySet()) {
      String column = entry.getKey();
      String className = entry.getValue();
      Pair<Text,Text> pcic = ColumnSet.decodeColumns(column);
      K inst = createInstance(context, className, c);

      if (pcic.getSecond() == null) {
        addObject(pcic.getFirst(), inst);
      } else {
        addObject(pcic.getFirst(), pcic.getSecond(), inst);
      }
    }
  }

  private K createInstance(String context, String className, Class<? extends K> c)
      throws ReflectiveOperationException, IOException {
    Class<? extends K> clazz = ClassLoaderUtil.loadClass(context, className, c);
    return clazz.getDeclaredConstructor().newInstance();
  }

  protected void addObject(Text colf, K obj) {
    objectsCF.put(new ColFamHashKey(new Text(colf)), obj);
  }

  protected void addObject(Text colf, Text colq, K obj) {
    objectsCol.put(new ColHashKey(colf, colq), obj);
  }

  public K getObject(Key key) {
    K obj = lookupObjectInColumns(key);
    if (obj == null) {
      obj = lookupObjectInColumnFamilies(key);
    }
    return obj;
  }

  private K lookupObjectInColumns(Key key) {
    if (!objectsCol.isEmpty()) {
      lookupCol.set(key);
      return objectsCol.get(lookupCol);
    }
    return null;
  }

  private K lookupObjectInColumnFamilies(Key key) {
    if (!objectsCF.isEmpty()) {
      lookupCF.set(key);
      return objectsCF.get(lookupCF);
    }
    return null;
  }

  public boolean isEmpty() {
    return objectsCol.isEmpty() && objectsCF.isEmpty();
  }
}
