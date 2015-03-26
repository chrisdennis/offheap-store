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

import java.util.Random;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.pinning.PinnableCache;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.DebuggingUtils;

import org.terracotta.offheapstore.data.IntData;
import org.terracotta.offheapstore.data.Source;

/**
 * An abstract off-heap cache implementation.
 * <p>
 * Subclasses must implement the two {@code getEvictionIndex(...)} methods to
 * instruct the cache regarding which mappings to remove.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 * @author Manoj Govindassamy
 */
public abstract class AbstractOffHeapClockCache<K, V> extends AbstractLockedOffHeapHashMap<K, V> implements PinnableCache<K, V>,PinnableSegment<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOffHeapClockCache.class);
  
  private static final int PRESENT_CLOCK = 1 << (Integer.SIZE - 1);

  private final Random rndm = new Random();

  private int clockHand;

  public AbstractOffHeapClockCache(Source<IntData> source, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, storageEngine);
  }

  public AbstractOffHeapClockCache(Source<IntData> source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, tableAllocationsSteal, storageEngine);
  }

  public AbstractOffHeapClockCache(Source<IntData> source, StorageEngine<? super K, ? super V> storageEngine, boolean bootstrap) {
    super(source, storageEngine, bootstrap);
  }

  public AbstractOffHeapClockCache(Source<IntData> source, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, storageEngine, tableSize);
  }
  
  public AbstractOffHeapClockCache(Source<IntData> source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, tableAllocationsSteal, storageEngine, tableSize);
  }
  
  public AbstractOffHeapClockCache(Source<IntData> source, StorageEngine<? super K, ? super V> storageEngine, int tableSize, boolean bootstrap) {
    super(source, storageEngine, tableSize, bootstrap);
  }

  @Override
  protected void storageEngineFailure(Object failure) {
    if (isEmpty()) {
      StringBuilder sb = new StringBuilder("Storage Engine and Eviction Failed.\n");
      sb.append("Storage Engine : ").append(storageEngine);
      throw new OversizeMappingException(sb.toString());
    } else {
      int evictionIndex = getEvictionIndex();
      if (evictionIndex < 0) {
        StringBuilder sb = new StringBuilder("Storage Engine and Eviction Failed.\n");
        sb.append("Storage Engine : ").append(storageEngine);
        throw new OversizeMappingException(sb.toString());
      } else {
        evict(evictionIndex, false);
      }
    }
  }

  @Override
  protected void tableExpansionFailure(long start, int length) {
    long evictionIndex = getEvictionIndex(start, length);
    if (evictionIndex < 0) {
      if (tryIncreaseReprobe()) {
        LOGGER.debug("Increased reprobe to {} slots for a {} slot table in a last ditch attempt to avoid storage failure.", getReprobeLength(), getTableCapacity());
      } else {
        StringBuilder sb = new StringBuilder("Table Expansion and Eviction Failed.\n");
        sb.append("Current Table Size (slots) : ").append(getTableCapacity()).append('\n');
        sb.append("Current Reprobe Length     : ").append(getReprobeLength()).append('\n');
        sb.append("Resize Will Require        : ").append(DebuggingUtils.toBase2SuffixedString(getTableCapacity() * ENTRY_SIZE * (Integer.SIZE / Byte.SIZE) * 2)).append("B\n");
        sb.append("Table Page Source          : ").append(tableSource);
        throw new OversizeMappingException(sb.toString());
      }
    } else {
      evict(evictionIndex, false);
    }
  }

  @Override
  protected void hit(IntData entry) {
    entry.put(STATUS, PRESENT_CLOCK | entry.get(STATUS));
  }

  /**
   * Return the table offset of the to be evicted mapping.
   * <p>
   * The mapping to be evicted can occur anywhere in this cache's table.
   *
   * @return table offset of the mapping to be evicted
   */
  public int getEvictionIndex() {
    /*
     * If the table has been shrunk then it's possible for the clock-hand to
     * point past the end of the table.  We cannot allow the initial-hand to
     * be equal to this otherwise we may never be able to terminate this loop.
     */
    if (clockHand >= hashtable.size()) {
      clockHand = 0;
    }
    
    int initialHand = clockHand;
    int loops = 0;
    while (true) {
      if ((clockHand += ENTRY_SIZE) + STATUS >= hashtable.size()) {
        clockHand = 0;
      }

      int hand = hashtable.get(clockHand + STATUS);

      if (evictable(hand) && ((hand & PRESENT_CLOCK) == 0)) {
        return clockHand;
      } else if ((hand & PRESENT_CLOCK) == PRESENT_CLOCK) {
        hashtable.put(clockHand + STATUS, hand & ~PRESENT_CLOCK);
      }

      if (initialHand == clockHand && ++loops == 2) {
        return -1;
      }
    }
  }

  /**
   * Return the table offset of the to be evicted mapping within the given probe
   * sequence.
   * <p>
   * The mapping to be evicted must occur within the given probe sequence.
   *
   * @param start initial slot in probe sequence
   * @param length number of slots in probe sequence
   * @return table offset of the mapping to be evicted
   */
  private long getEvictionIndex(long start, int length) {
    /*
     * TODO This pseudo clock eviction may not be the best algorithm.  Currently
     * the algorithm is to do a regular eviction selection, which helps the clock
     * by advancing it, and also some clock-bit clearing.  If the resultant index
     * is in our probe sequence (which isn't the most simple of calculations - I
     * think I have it right here) then just return that as the eviction index,
     * and all is good.  If that index is not in our probe sequence, then we evict
     * that element anyway, we then go on to see if any of the entries in our
     * probe sequence have their clock bit cleared, if so we evict one of them to
     * clear space for the incoming element.  If all elements have set clock bits
     * then we just kick one out at random.
     */
    long index = getEvictionIndex();

    long tableLength = hashtable.size();
    int probeLength = length * ENTRY_SIZE;

    if (probeLength >= hashtable.size()) {
      return index;
    }

    long end = (start + probeLength) & (tableLength - 1);

    if (index < 0) {
      return index;
    } else if ((end > start) && (index >= start) && (index <= end)) {
      return index;
    } else if ((end < start) && ((index >= start) || (index < end))) {
      return index;
    } else {
      evict(index, false);

      long clock = start;
      for (int i = 0; i < length; i++) {
        if ((clock += ENTRY_SIZE) >= tableLength) {
          clock = start;
        }

        int hand = hashtable.get(clock + STATUS);

        if (evictable(hand) && ((hand & PRESENT_CLOCK) == 0)) {
          return clock;
        }
      }
      
      long lastEvictable = -1;
      clock = start;
      for (int i = 0; i < rndm.nextInt(length) || (lastEvictable < 0 && i < length); i++) {
        if ((clock += ENTRY_SIZE) >= tableLength) {
          clock = start;
        }

        int hand = hashtable.get(clock + STATUS);

        if (evictable(hand)) {
          lastEvictable = clock;
        }
      }
      return lastEvictable;
    }
  }

  protected boolean evictable(int status) {
    return (status & STATUS_USED) == STATUS_USED && ((status & Metadata.PINNED) == 0);
  }

  @Override
  public boolean evict(long index, boolean shrink) {
    Lock l = writeLock();
    l.lock();
    try {
      if (evictable(hashtable.get(index + STATUS))) {
        removeAtTableOffset(index, shrink);
        return true;
      } else {
        return false;
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean isPinned(Object key) {
    Lock l = readLock();
    l.lock();
    try {
      return (getMetadata(key) & Metadata.PINNED) == Metadata.PINNED;
    } finally {
      l.unlock();
    }
  }

  @Override
  public void setPinning(K key, boolean pinned) {
    Lock l = writeLock();
    l.lock();
    try {
      if (pinned) {
        setMetadata(key, Metadata.PINNED, Metadata.PINNED);
      } else {
        setMetadata(key, Metadata.PINNED, 0);
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public V putPinned(final K key, final V value) {
    return put(key, value, Metadata.PINNED);
  }
}
