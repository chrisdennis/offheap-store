/* 
 * Copyright 2015 Terracotta, Inc., a Software AG company.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.offheapstore;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.Predicate;

import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

/**
 * An abstract locked off-heap map.
 * <p>
 * Subclasses must implement the {@code readLock()} and {@code writeLock()}
 * methods such that they return the correct locks under which read and write
 * operations must occur.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public abstract class AbstractLockedOffHeapHashMap<K, V> extends OffHeapHashMap<K, V> implements Segment<K, V> {

  private Set<Entry<K, V>> entrySet;
  private Set<K> keySet;
  private Collection<V> values;

  public AbstractLockedOffHeapHashMap(PageSource source, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, storageEngine);
  }

  public AbstractLockedOffHeapHashMap(PageSource source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, tableAllocationsSteal, storageEngine);
  }

  public AbstractLockedOffHeapHashMap(PageSource source, StorageEngine<? super K, ? super V> storageEngine, boolean bootstrap) {
    super(source, storageEngine, bootstrap);
  }

  public AbstractLockedOffHeapHashMap(PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, storageEngine, tableSize);
  }

  public AbstractLockedOffHeapHashMap(PageSource source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, tableAllocationsSteal, storageEngine, tableSize);
  }

  public AbstractLockedOffHeapHashMap(PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize, boolean bootstrap) {
    super(source, false, storageEngine, tableSize, bootstrap);
  }

  @Override
  public int size() {
    Lock l = readLock();
    l.lock();
    try {
      return super.size();
    } finally {
      l.unlock();
    }
  }

  @Override
  protected <T> T read(int hash, Predicate<IntBuffer> slotCheck, Function<Optional<IntBuffer>, T> outputTransform) {
    Lock l = readLock();
    l.lock();
    try {
      return super.read(hash, slotCheck, outputTransform);
    } finally {
      l.unlock();
    }
  }
  
  @Override
  public long installMappingForHashAndEncoding(int pojoHash, ByteBuffer offheapBinaryKey, ByteBuffer offheapBinaryValue, int metadata) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.installMappingForHashAndEncoding(pojoHash, offheapBinaryKey, offheapBinaryValue, metadata);
    } finally {
      l.unlock();
    }
  }
  
  @Override
  public V put(K key, V value) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.put(key, value);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V put(K key, V value, int metadata) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.put(key, value, metadata);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V fill(K key, V value) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.fill(key, value);
    } finally {
      l.unlock();
    }
  }
  
  @Override
  public V fill(K key, V value, int metadata) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.fill(key, value, metadata);
    } finally {
      l.unlock();
    }
  }

  @Override
  protected <T> T remove(int hash, Predicate<IntBuffer> slotCheck, Function<Optional<IntBuffer>, T> output) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.remove(hash, slotCheck, output);
    } finally {
      l.unlock();
    }
  }

  
  @Override
  public void clear() {
    Lock l = writeLock();
    l.lock();
    try {
      super.clear();
    } finally {
      l.unlock();
    }
  }

  @Override
  public V putIfAbsent(K key, V value) {
    Lock l = writeLock();
    l.lock();
    try {
      if (key == null || value == null) {
        throw new NullPointerException();
      }
      V existing = get(key);
      if (existing == null) {
        put(key, value);
      }
      return existing;
    } finally {
      l.unlock();
    }
  }

  @Override
  public final boolean remove(Object key, Object value) {
    return remove(key.hashCode(), keyEquality(key).and(valueEquality(value)), Optional::isPresent);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    Lock l = writeLock();
    l.lock();
    try {
      V existing = get(key);
      if (oldValue.equals(existing)) {
        put(key, newValue);
        return true;
      } else {
        return false;
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public V replace(K key, V value) {
    Lock l = writeLock();
    l.lock();
    try {
      if (value == null || key == null) {
        throw new NullPointerException();
      }
      V existing = get(key);
      if (existing != null) {
        put(key, value);
      }
      return existing;
    } finally {
      l.unlock();
    }
  }

  @Override
  public Integer getMetadata(Object key, int mask) {
    Lock l = readLock();
    l.lock();
    try {
      return super.getMetadata(key, mask);
    } finally {
      l.unlock();
    }
  }
  
  @Override
  public Integer getAndSetMetadata(Object key, int mask, int values) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.getAndSetMetadata(key, mask, values);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V getValueAndSetMetadata(Object key, int mask, int values) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.getValueAndSetMetadata(key, mask, values);
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean evict(int index, boolean shrink) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.evict(index, shrink);
    } finally {
      l.unlock();
    }
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Set<Entry<K, V>> es = entrySet;
    return es == null ? (entrySet = new LockedEntrySet()) : es;
  }

  class LockedEntrySet extends AbstractSet<Entry<K, V>> {

    @Override
    public Iterator<Entry<K, V>> iterator() {
      Lock l = readLock();
      l.lock();
      try {
        return new LockedHashIterator<>(DirectEntry::new);
      } finally {
        l.unlock();
      }
    }

    @Override
    public boolean contains(Object o) {
      if (!(o instanceof Entry<?, ?>)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      Entry<K, V> e = (Entry<K, V>) o;
      Lock l = readLock();
      l.lock();
      try {
        V value = AbstractLockedOffHeapHashMap.this.get(e.getKey());
        return value != null && value.equals(e.getValue());
      } finally {
        l.unlock();
      }
    }

    @Override
    public boolean remove(Object o) {
      return AbstractLockedOffHeapHashMap.this.removeMapping(o);
    }

    @Override
    public int size() {
      return AbstractLockedOffHeapHashMap.this.size();
    }

    @Override
    public void clear() {
      AbstractLockedOffHeapHashMap.this.clear();
    }
  }

  class LockedHashIterator<T> extends HashIterator<T> {

    public LockedHashIterator(Function<IntBuffer, T> create) {
      super(create);
    }

    @Override
    public T next() {
      Lock l = readLock();
      l.lock();
      try {
        return super.next();
      } finally {
        l.unlock();
      }
    }

    @Override
    public void remove() {
      Lock l = writeLock();
      l.lock();
      try {
        super.remove();
      } finally {
        l.unlock();
      }
    }

    @Override
    protected void checkForConcurrentModification() {
      //no-op
    }
  }

  @Override
  public Set<K> keySet() {
    if (keySet == null) {
      keySet = new LockedKeySet();
    }
    return keySet;
  }

  class LockedKeySet extends AbstractSet<K> {

    @Override
    public Iterator<K> iterator() {
      Lock l = readLock();
      l.lock();
      try {
        return new LockedHashIterator(keyReader());
      } finally {
        l.unlock();
      }
    }

    @Override
    public boolean contains(Object o) {
      return AbstractLockedOffHeapHashMap.this.containsKey(o);
    }

    @Override
    public boolean remove(Object o) {
      return AbstractLockedOffHeapHashMap.this.remove(o) != null;
    }

    @Override
    public int size() {
      return AbstractLockedOffHeapHashMap.this.size();
    }

    @Override
    public void clear() {
      AbstractLockedOffHeapHashMap.this.clear();
    }
  }

  @Override
  public Collection<V> values() {
    if (values == null) {
      values = new AbstractCollection<V>() {

        @Override
        public Iterator<V> iterator() {
          return new Iterator<V>() {

            private final Iterator<Entry<K, V>> i = entrySet().iterator();

            @Override
            public boolean hasNext() {
              return i.hasNext();
            }

            @Override
            public V next() {
              return i.next().getValue();
            }

            @Override
            public void remove() {
              i.remove();
            }
          };
        }

        @Override
        public int size() {
          return AbstractLockedOffHeapHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
          return AbstractLockedOffHeapHashMap.this.isEmpty();
        }

        @Override
        public void clear() {
          AbstractLockedOffHeapHashMap.this.clear();
        }

        @Override
        public boolean contains(Object v) {
          return AbstractLockedOffHeapHashMap.this.containsValue(v);
        }
      };
    }
    return values;
  }

  @Override
  public void destroy() {
    Lock l = writeLock();
    l.lock();
    try {
      super.destroy();
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean shrink() {
    Lock l = writeLock();
    l.lock();
    try {
      return storageEngine.shrink();
    } finally {
      l.unlock();
    }
  }

  @Override
  public abstract Lock readLock();
  
  @Override
  public abstract Lock writeLock();
}
