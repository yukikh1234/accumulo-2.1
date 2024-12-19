
package org.apache.accumulo.core.iteratorsImpl.system;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.mutable.MutableLong;

public class LocalityGroupIterator extends HeapIterator implements InterruptibleIterator {

  private static final Collection<ByteSequence> EMPTY_CF_SET = Collections.emptySet();

  public static class LocalityGroup {
    private InterruptibleIterator iterator;
    protected boolean isDefaultLocalityGroup;
    protected Map<ByteSequence,MutableLong> columnFamilies;

    private LocalityGroup(LocalityGroup localityGroup, IteratorEnvironment env) {
      this(localityGroup.columnFamilies, localityGroup.isDefaultLocalityGroup);
      this.iterator = (InterruptibleIterator) localityGroup.iterator.deepCopy(env);
    }

    public LocalityGroup(InterruptibleIterator iterator,
        Map<ByteSequence,MutableLong> columnFamilies, boolean isDefaultLocalityGroup) {
      this(columnFamilies, isDefaultLocalityGroup);
      this.iterator = iterator;
    }

    public LocalityGroup(Map<ByteSequence,MutableLong> columnFamilies,
        boolean isDefaultLocalityGroup) {
      this.isDefaultLocalityGroup = isDefaultLocalityGroup;
      this.columnFamilies = columnFamilies;
    }

    public InterruptibleIterator getIterator() {
      return iterator;
    }
  }

  public static class LocalityGroupContext {
    final List<LocalityGroup> groups;
    final LocalityGroup defaultGroup;
    final Map<ByteSequence,LocalityGroup> groupByCf;

    public LocalityGroupContext(LocalityGroup[] groups) {
      this.groups = Collections.unmodifiableList(Arrays.asList(groups));
      this.groupByCf = new HashMap<>();
      LocalityGroup foundDefault = null;

      for (LocalityGroup group : groups) {
        if (group.isDefaultLocalityGroup && group.columnFamilies == null) {
          if (foundDefault != null) {
            throw new IllegalStateException("Found multiple default locality groups");
          }
          foundDefault = group;
        } else {
          for (Entry<ByteSequence,MutableLong> entry : group.columnFamilies.entrySet()) {
            if (entry.getValue().longValue() > 0) {
              if (groupByCf.containsKey(entry.getKey())) {
                throw new IllegalStateException("Found the same cf in multiple locality groups");
              }
              groupByCf.put(entry.getKey(), group);
            }
          }
        }
      }
      defaultGroup = foundDefault;
    }
  }

  public static class LocalityGroupSeekCache {
    private Set<ByteSequence> lastColumnFamilies;
    private volatile boolean lastInclusive;
    private Collection<LocalityGroup> lastUsed;

    public Set<ByteSequence> getLastColumnFamilies() {
      return lastColumnFamilies;
    }

    public boolean isLastInclusive() {
      return lastInclusive;
    }

    public Collection<LocalityGroup> getLastUsed() {
      return lastUsed;
    }

    public int getNumLGSeeked() {
      return (lastUsed == null ? 0 : lastUsed.size());
    }
  }

  private final LocalityGroupContext lgContext;
  private LocalityGroupSeekCache lgCache;
  private AtomicBoolean interruptFlag;

  public LocalityGroupIterator(LocalityGroup[] groups) {
    super(groups.length);
    this.lgContext = new LocalityGroupContext(groups);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  static final Collection<LocalityGroup> _seek(HeapIterator hiter, LocalityGroupContext lgContext,
      Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    hiter.clear();
    final Set<ByteSequence> cfSet = getCfSet(columnFamilies);
    final Collection<LocalityGroup> groups = getLocalityGroups(lgContext, inclusive, cfSet);

    for (LocalityGroup lgr : groups) {
      lgr.getIterator().seek(range, EMPTY_CF_SET, false);
      hiter.addSource(lgr.getIterator());
    }

    return groups;
  }

  private static Collection<LocalityGroup> getLocalityGroups(LocalityGroupContext lgContext,
      boolean inclusive, Set<ByteSequence> cfSet) {

    final Collection<LocalityGroup> groups = new HashSet<>();
    if (cfSet.isEmpty()) {
      if (!inclusive)
        groups.addAll(lgContext.groups);
    } else {
      if (lgContext.defaultGroup != null) {
        if (inclusive && !lgContext.groupByCf.keySet().containsAll(cfSet)) {
          groups.add(lgContext.defaultGroup);
        } else if (!inclusive) {
          groups.add(lgContext.defaultGroup);
        }
      }
      addRelevantGroups(lgContext, inclusive, cfSet, groups);
    }
    return groups;
  }

  private static void addRelevantGroups(LocalityGroupContext lgContext, boolean inclusive,
      Set<ByteSequence> cfSet, Collection<LocalityGroup> groups) {
    if (!inclusive) {
      lgContext.groupByCf.entrySet().stream().filter(entry -> !cfSet.contains(entry.getKey()))
          .map(Entry::getValue).forEach(groups::add);
    } else if (lgContext.groupByCf.size() <= cfSet.size()) {
      lgContext.groupByCf.entrySet().stream().filter(entry -> cfSet.contains(entry.getKey()))
          .map(Entry::getValue).forEach(groups::add);
    } else {
      cfSet.stream().map(lgContext.groupByCf::get).filter(Objects::nonNull).forEach(groups::add);
    }
  }

  private static Set<ByteSequence> getCfSet(Collection<ByteSequence> columnFamilies) {
    if (columnFamilies.isEmpty()) {
      return Collections.emptySet();
    } else if (columnFamilies instanceof Set<?>) {
      return (Set<ByteSequence>) columnFamilies;
    } else {
      return Set.copyOf(columnFamilies);
    }
  }

  public static LocalityGroupSeekCache seek(HeapIterator hiter, LocalityGroupContext lgContext,
      Range range, Collection<ByteSequence> columnFamilies, boolean inclusive,
      LocalityGroupSeekCache lgSeekCache) throws IOException {
    if (lgSeekCache == null) {
      lgSeekCache = new LocalityGroupSeekCache();
    }

    boolean sameArgs = areSeekArgsSame(columnFamilies, inclusive, lgSeekCache);
    if (sameArgs) {
      reseek(hiter, range, lgSeekCache);
    } else {
      updateSeekCache(hiter, lgContext, range, columnFamilies, inclusive, lgSeekCache);
    }

    return lgSeekCache;
  }

  private static boolean areSeekArgsSame(Collection<ByteSequence> columnFamilies, boolean inclusive,
      LocalityGroupSeekCache lgSeekCache) {
    boolean sameArgs = false;
    if (lgSeekCache.lastUsed != null && inclusive == lgSeekCache.lastInclusive) {
      if (columnFamilies instanceof Set) {
        sameArgs = lgSeekCache.lastColumnFamilies.equals(columnFamilies);
      } else {
        Set<ByteSequence> cfSet = Set.copyOf(columnFamilies);
        sameArgs = lgSeekCache.lastColumnFamilies.equals(cfSet);
      }
    }
    return sameArgs;
  }

  private static void reseek(HeapIterator hiter, Range range, LocalityGroupSeekCache lgSeekCache)
      throws IOException {
    hiter.clear();
    for (LocalityGroup lgr : lgSeekCache.lastUsed) {
      lgr.getIterator().seek(range, EMPTY_CF_SET, false);
      hiter.addSource(lgr.getIterator());
    }
  }

  private static void updateSeekCache(HeapIterator hiter, LocalityGroupContext lgContext,
      Range range, Collection<ByteSequence> columnFamilies, boolean inclusive,
      LocalityGroupSeekCache lgSeekCache) throws IOException {
    Set<ByteSequence> cfSet = Set.copyOf(columnFamilies);
    lgSeekCache.lastColumnFamilies = cfSet;
    lgSeekCache.lastInclusive = inclusive;
    lgSeekCache.lastUsed = _seek(hiter, lgContext, range, columnFamilies, inclusive);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    lgCache = seek(this, lgContext, range, columnFamilies, inclusive, lgCache);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    LocalityGroup[] groupsCopy = new LocalityGroup[lgContext.groups.size()];

    for (int i = 0; i < lgContext.groups.size(); i++) {
      groupsCopy[i] = new LocalityGroup(lgContext.groups.get(i), env);
      if (interruptFlag != null) {
        groupsCopy[i].getIterator().setInterruptFlag(interruptFlag);
      }
    }

    return new LocalityGroupIterator(groupsCopy);
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    this.interruptFlag = flag;
    for (LocalityGroup lgr : lgContext.groups) {
      lgr.getIterator().setInterruptFlag(flag);
    }
  }
}
