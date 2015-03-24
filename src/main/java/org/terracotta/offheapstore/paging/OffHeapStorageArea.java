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
package org.terracotta.offheapstore.paging;

import static org.terracotta.offheapstore.util.DebuggingUtils.toBase2SuffixedString;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.allocator.Allocator;
import org.terracotta.offheapstore.storage.allocator.IntegerBestFitAllocator;
import org.terracotta.offheapstore.storage.allocator.LongBestFitAllocator;

import java.util.Deque;
import java.util.Random;
import org.terracotta.offheapstore.data.ByteData;
import org.terracotta.offheapstore.data.Data;
import org.terracotta.offheapstore.data.Source;

import static org.terracotta.offheapstore.util.Validation.shouldValidate;
import static org.terracotta.offheapstore.util.Validation.validate;

/**
 *
 * @author cdennis
 */
public class OffHeapStorageArea {

  private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapStorageArea.class);
  private static final boolean VALIDATING = shouldValidate(OffHeapStorageArea.class);
  private static final long LARGEST_POWER_OF_TWO = Integer.highestOneBit(Integer.MAX_VALUE);

  private final int initialPageSize;
  private final int maximalPageSize;
  private final int pageGrowthAreaSize;
  private final float compressThreshold;

  private final Owner owner;
  private final Source<ByteData> pageSource;
  private final Allocator allocator;
  private final Random random = new Random();
  
  private Deque<Collection<ByteData>> released = new LinkedList<Collection<ByteData>>();

  /*
   * This map is only accessed by one thread on write due to write exclusion at
   * the AbstractLockedOffHeapHashMap (segment) level so one stripe is
   * sufficient. Switching to a Hashtable/Collections.synchronizedMap(...) would
   * be bad however as we need concurrent read access still.
   */
  private final Map<Integer, ByteData> pages = new ConcurrentHashMap<Integer, ByteData>(1, 0.75f, 1);

  private final boolean thief;
  private final boolean victim;
  
  public OffHeapStorageArea(PointerSize width, Owner owner, Source<ByteData> pageSource, int pageSize, boolean thief, boolean victim) {
    this(width, owner, pageSource, pageSize, pageSize, thief, victim);
  }

  public OffHeapStorageArea(PointerSize width, Owner owner, Source<ByteData> pageSource, int pageSize, boolean thief, boolean victim, float compressThreshold) {
    this(width, owner, pageSource, pageSize, pageSize, thief, victim, compressThreshold);
  }

  public OffHeapStorageArea(PointerSize width, Owner owner, Source<ByteData> pageSource, int initialPageSize, int maximalPageSize, boolean thief, boolean victim) {
    this(width, owner, pageSource, initialPageSize, maximalPageSize, thief, victim, 0.0f);
  }

  public OffHeapStorageArea(PointerSize width, Owner owner, Source<ByteData> pageSource, int initialPageSize, int maximalPageSize, boolean thief, boolean victim, float compressThreshold) {
    if (victim && maximalPageSize != initialPageSize) {
      throw new IllegalArgumentException("Variable page-size offheap storage areas cannot be victims as they do not support stealing.");
    }
    
    this.owner = owner;
    this.pageSource = pageSource;
    
    switch (width) {
      case INT:
        this.allocator = new IntegerBestFitAllocator(this);
        break;
      case LONG:
        this.allocator = new LongBestFitAllocator(this);
        break;
      default:
        throw new UnsupportedOperationException();
    }

    initialPageSize = Math.max(allocator.getMinimalSize(), initialPageSize);
    if (Integer.bitCount(initialPageSize) == 1) {
      this.initialPageSize = (int) Math.min(LARGEST_POWER_OF_TWO, initialPageSize);
    } else {
      this.initialPageSize = (int) Math.min(LARGEST_POWER_OF_TWO, Long.highestOneBit(initialPageSize) << 1);
    }
    if (maximalPageSize < initialPageSize) {
      this.maximalPageSize = initialPageSize;
    } else if (Integer.bitCount(maximalPageSize) == 1) {
      this.maximalPageSize = (int) Math.min(LARGEST_POWER_OF_TWO, maximalPageSize);
    } else {
      this.maximalPageSize = (int) Math.min(LARGEST_POWER_OF_TWO, Long.highestOneBit(maximalPageSize) << 1);
    }
    this.pageGrowthAreaSize = this.maximalPageSize - this.initialPageSize;
    this.compressThreshold = compressThreshold;
    this.thief = thief;
    this.victim = victim;
  }

  public void clear() {
    allocator.clear();
    for (Iterator<ByteData> it = pages.values().iterator(); it.hasNext(); ) {
      ByteData p = it.next();
      it.remove();
      freePage(p);
    }
    validatePages();
  }

  public byte readByte(long address) {
    int pageIndex = pageIndexFor(address);
    int pageAddress = pageAddressFor(address);

    return pages.get(pageIndex).asByteBuffer().get(pageAddress);
  }
  
  public short readShort(long address) {
    int pageIndex = pageIndexFor(address);
    int pageAddress = pageAddressFor(address);
    int pageSize = pageSizeFor(pageIndex);

    if (pageAddress + 2 <= pageSize) {
      return pages.get(pageIndex).asByteBuffer().getShort(pageAddress);
    } else {
      short value = 0;
      for (int i = 0; i < 2; i++) {
        ByteBuffer buffer = pages.get(pageIndex).asByteBuffer();
        value |= (0xff & buffer.get(pageAddress)) << (Byte.SIZE * (1 - i));
        address++;
        pageIndex = pageIndexFor(address);
        pageAddress = pageAddressFor(address);
      }
      return value;
    }
  }
  
  public int readInt(long address) {
    int pageIndex = pageIndexFor(address);
    int pageAddress = pageAddressFor(address);
    int pageSize = pageSizeFor(pageIndex);

    if (pageAddress + 4 <= pageSize) {
      return pages.get(pageIndex).getInt(pageAddress);
    } else {
      int value = 0;
      for (int i = 0; i < 4; i++) {
        ByteData buffer = pages.get(pageIndex);
        value |= (0xff & buffer.get(pageAddress)) << (Byte.SIZE * (3 - i));
        address++;
        pageIndex = pageIndexFor(address);
        pageAddress = pageAddressFor(address);
      }
      return value;
    }
  }

  public long readLong(long address) {
    int pageIndex = pageIndexFor(address);
    int pageAddress = pageAddressFor(address);
    int pageSize = pageSizeFor(pageIndex);

    if (pageAddress + 8 <= pageSize) {
      return pages.get(pageIndex).getLong(pageAddress);
    } else {
      long value = 0;
      for (int i = 0; i < 8; i++) {
        ByteData buffer = pages.get(pageIndex);
        value |= (0xffL & buffer.get(pageAddress)) << (Byte.SIZE * (7 - i));
        address++;
        pageIndex = pageIndexFor(address);
        pageAddress = pageAddressFor(address);
      }
      return value;
    }
  }

  public ByteData readBuffer(long address, int length) {
    int pageIndex = pageIndexFor(address);
    int pageAddress = pageAddressFor(address);
    int pageSize = pageSizeFor(pageIndex);

    if (pageAddress + length <= pageSize) {
      ByteData buffer = pages.get(pageIndex);
      return buffer.slice(pageAddress, length).asReadOnly();
    } else {
      ByteData data = Data.allocate(length);
      while (data.hasRemaining()) {
        ByteData buffer = pages.get(pageIndex).slice(pageAddress, address);
        buffer.position(pageAddress);
        if (buffer.remaining() > data.remaining()) {
          buffer.limit(buffer.position() + data.remaining());
        }
        address += buffer.remaining();
        data.put(buffer);
        pageIndex = pageIndexFor(address);
        pageAddress = pageAddressFor(address);
      }
      return ((ByteBuffer) data.flip()).asReadOnlyBuffer();
    }
  }

  public void writeByte(long address, byte value) {
    int pageIndex = pageIndexFor(address);
    int pageAddress = pageAddressFor(address);

    pages.get(pageIndex).asByteBuffer().put(pageAddress, value);
  }
  
  public void writeShort(long address, short value) {
    int pageIndex = pageIndexFor(address);
    int pageAddress = pageAddressFor(address);
    int pageSize = pageSizeFor(pageIndex);

    if (pageAddress + 2 <= pageSize) {
      pages.get(pageIndex).asByteBuffer().putShort(pageAddress, value);
    } else {
      for (int i = 0; i < 2; i++) {
        ByteBuffer buffer = pages.get(pageIndex).asByteBuffer();
        buffer.position(pageAddress);
        buffer.put((byte) (value >> (Byte.SIZE * (1 - i))));
        address++;
        pageIndex = pageIndexFor(address);
        pageAddress = pageAddressFor(address);
      }
    }
  }
  
  public void writeInt(long address, int value) {
    int pageIndex = pageIndexFor(address);
    int pageAddress = pageAddressFor(address);
    int pageSize = pageSizeFor(pageIndex);

    if (pageAddress + 4 <= pageSize) {
      pages.get(pageIndex).putInt(pageAddress, value);
    } else {
      for (int i = 0; i < 4; i++) {
        ByteBuffer buffer = pages.get(pageIndex).asByteBuffer();
        buffer.position(pageAddress);
        buffer.put((byte) (value >> (Byte.SIZE * (3 - i))));
        address++;
        pageIndex = pageIndexFor(address);
        pageAddress = pageAddressFor(address);
      }
    }
  }

  public void writeLong(long address, long value) {
    int pageIndex = pageIndexFor(address);
    int pageAddress = pageAddressFor(address);
    int pageSize = pageSizeFor(pageIndex);

    if (pageAddress + 8 <= pageSize) {
      pages.get(pageIndex).putLong(pageAddress, value);
    } else {
      for (int i = 0; i < 8; i++) {
        ByteBuffer buffer = pages.get(pageIndex).asByteBuffer();
        buffer.position(pageAddress);
        buffer.put((byte) (value >> (Byte.SIZE * (7 - i))));
        address++;
        pageIndex = pageIndexFor(address);
        pageAddress = pageAddressFor(address);
      }
    }
  }
  
  public void writeBuffer(long address, ByteData data) {
    int pageIndex = pageIndexFor(address);
    int pageAddress = pageAddressFor(address);
    int pageSize = pageSizeFor(pageIndex);

    if (pageAddress + data.remaining() <= pageSize) {
      ByteBuffer buffer = pages.get(pageIndex).asByteBuffer();
      buffer.position(pageAddress);
      buffer.put(data);
    } else {
      while (data.hasRemaining()) {
        ByteBuffer buffer = pages.get(pageIndex).asByteBuffer();
        buffer.position(pageAddress);
        if (data.remaining() > buffer.remaining()) {
          int originalLimit = data.limit();
          try {
            data.limit(data.position() + buffer.remaining());
            address += data.remaining();
            buffer.put(data);
          } finally {
            data.limit(originalLimit);
          }
        } else {
          address += data.remaining();
          buffer.put(data);
        }
        pageIndex = pageIndexFor(address);
        pageAddress = pageAddressFor(address);
      }
    }
  }

  public void writeBuffers(long address, ByteData[] data) {
    for (ByteData buffer : data) {
      int length = buffer.remaining();
      writeBuffer(address, buffer);
      address += length;
    }
  }
  
  public void free(long address) {
    allocator.free(address);
    if (compressThreshold > 0) {
      float occupation = ((float) getOccupiedMemory()) / allocator.getLastUsedAddress();
      if (occupation < compressThreshold) {
        compress();
      }
    }
  }
  
  private boolean compress() {
    long lastAddress = allocator.getLastUsedPointer();
    int sizeOfArea = owner.sizeOf(lastAddress);
    
    long compressed = allocator.allocate(sizeOfArea);
    if (compressed >= 0) {
      if (compressed < lastAddress) {
        writeBuffer(compressed, readBuffer(lastAddress, sizeOfArea));
        if (owner.moved(lastAddress, compressed)) {
          allocator.free(lastAddress);
          return true;
        }
      }
      allocator.free(compressed);
      return false;
    } else {
      return false;
    }
  }

  public void destroy() {
    allocator.clear();
    for (Iterator<ByteData> it = pages.values().iterator(); it.hasNext(); ) {
      ByteData p = it.next();
      it.remove();
      freePage(p);
    }
    validatePages();
  }

  public long allocate(long size) {
    while (true) {
      long address = allocator.allocate(size);
      if (address >= 0) {
        return address;
      } else if (!expandData()) {
        return -1L;
      }
    }
  }

  private boolean expandData() {
    int newPageSize = nextPageSize();
    if (getAllocatedMemory() + newPageSize > allocator.getMaximumAddress()) {
      return false;
    }
    ByteData newPage = pageSource.allocate(newPageSize, thief, victim, this);
    if (newPage == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Data area expansion from {} failed", getAllocatedMemory());
      }
      return false;
    } else if (pages.put(pages.size(), newPage) == null) {
      validatePages();
      allocator.expand(newPageSize);
      if (LOGGER.isDebugEnabled()) {
        long before = getAllocatedMemory();
        long after = before + newPageSize;
        LOGGER.debug("Data area expanded from {}B to {}B [occupation={}]",
            new Object[] {toBase2SuffixedString(before), toBase2SuffixedString(after),
            ((float) allocator.occupied()) / after});
      }
      return true;
    } else {
      freePage(newPage);
      validatePages();
      throw new AssertionError();
    }
  }

  public long getAllocatedMemory() {
    return addressForPage(pages.size());
  }

  public long getOccupiedMemory() {
    return allocator.occupied();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("OffHeapStorageArea\n");
    for (int i = 0; i < pages.size(); ) {
      ByteData p = pages.get(i++);
      if (p == null) {
        break;
      } else {
        long size = p.size();
        int count = 1;
        while (i < pages.size()) {
          ByteData q = pages.get(i);
          if (q != null && q.size() == size) {
            count++;
            i++;
          } else {
            break;
          }
        }
        sb.append("\t").append(count).append(" ").append(toBase2SuffixedString(size)).append("B page").append(count == 1 ? "\n" : "s\n");
      }
    }
    sb.append("Allocator: ").append(allocator).append('\n');
    sb.append("Page Source: ").append(pageSource);
    return sb.toString();
  }

  private int pageIndexFor(long address) {
    if (address > pageGrowthAreaSize) {
      return (int) ((address - pageGrowthAreaSize) / maximalPageSize + pageIndexFor(pageGrowthAreaSize));
    } else {
      return Long.SIZE - Long.numberOfLeadingZeros((address / initialPageSize) + 1) - 1;
    }
  }

  private long addressForPage(int index) {
    int postIndex = index - pageIndexFor(pageGrowthAreaSize);
    if (postIndex > 0) {
      return pageGrowthAreaSize + (((long) maximalPageSize) * postIndex);
    } else {
      return (initialPageSize << index) - initialPageSize;
    }
  }

  private int pageAddressFor(long address) {
    return (int) (address - addressForPage(pageIndexFor(address)));
  }

  private int pageSizeFor(int index) {
    if (index < pageIndexFor(pageGrowthAreaSize)) {
      return initialPageSize << index;
    } else {
      return maximalPageSize;
    }
  }

  private int nextPageSize() {
    return pageSizeFor(pages.size());
  }

  public void validateStorageArea() {
    allocator.validateAllocator();
  }

  public void release(long address) {
    int lastPage = pageIndexFor(address);

    for (int i = pages.size() - 1; i > lastPage; i--) {
      ByteData p = pages.remove(i);
      allocator.expand(-p.size());
      freePage(p);
    }
    validatePages();
  }

  private void freePage(ByteData p) {
    if (released.isEmpty()) {
      pageSource.free(p);
    } else {
      released.peek().add(p);
    }
  }

  public Collection<ByteData> release(Collection<ByteData> targets) {
    /*
     * TODO This locking might be too coarse grained - can we safely allow
     * threads in to the map while we do this release process?
     */
    final Lock ownerLock = owner.writeLock();
    if (thief || owner.isThief()) {
      if (!ownerLock.tryLock()) {
        return Collections.emptyList();
      }
    } else {
      ownerLock.lock();
    }
    try {
      Collection<ByteData> recovered = new LinkedList<ByteData>();
      Collection<ByteData> freed = new LinkedList<ByteData>();
      /**
       * iterate backwards from top, and free until top is beneath tail page.
       */
      while (freed.size() < targets.size()) {
        long remove = allocator.getLastUsedPointer();
        if (remove < 0) {
          for (int i = pages.size() - 1; i >= 0; i--) {
            ByteData free = pages.get(i);
            allocator.expand(-free.size());
            pages.remove(i);
            if (targets.remove(free)) {
              recovered.add(free);
            } else {
              freed.add(free);
            }
          }
          validatePages();
          break;
        } else {
          Collection<ByteData> releasedPages = new ArrayList<ByteData>();
          released.push(releasedPages);
          try {
            if (owner.evictAtAddress(remove, true) || moveAddressDown(remove)) {
              for (ByteData p : releasedPages) {
                if (targets.remove(p)) {
                  recovered.add(p);
                } else {
                  freed.add(p);
                }
              }
              validatePages();
            } else if (releasedPages.isEmpty()) {
              break;
            } else {
              throw new AssertionError();
            }
          } finally {
            released.pop();
          }
        }
      }

      Iterator<ByteData> freePageSource = freed.iterator();
      for (ByteData t : targets) {
        int index = getIndexForPage(t);
        if (index >= 0 && freePageSource.hasNext()) {
          ByteData f = freePageSource.next();
          validate(!VALIDATING || f != t);
          validate(!VALIDATING || f.size() == t.size());
          f.put(0, t);
          pages.put(index, f);
          recovered.add(t);
        }
      }
      validatePages();

      while (freePageSource.hasNext()) {
        freePage(freePageSource.next());
      }

      return recovered;
    } finally {
      ownerLock.unlock();
    }
  }

  private boolean moveAddressDown(long target) {
    //we must move this address to a new location
    int sizeOfArea = owner.sizeOf(target);
    
    long ceiling = addressForPage(Math.max(0, pageIndexFor(target) - 2)) + 1;
    long startAt = random.nextLong() % ceiling; //check for negative results??
    
    Iterator<Long> pointers = allocator.iterator();
    
    while (pointers.hasNext() && pointers.next() < startAt);

    while (pointers.hasNext()) {
      long address = pointers.next();
      if (address < target && owner.evictAtAddress(address, false)) {
        long relocated = allocator.allocate(sizeOfArea);
        if (relocated >= 0) {
          if (relocated < target) {
            writeBuffer(relocated, readBuffer(target, sizeOfArea));
            if (!owner.moved(target, relocated)) {
              throw new AssertionError("Failure to move mapping during release");
            }
            allocator.free(target);
            return true;
          } else {
            allocator.free(relocated);
          }
        }
      }
    }
    
    LOGGER.debug("Random Eviction Failure Migration Failed - Using Biased Approach");
    
    for (long address : allocator) {
      if (address < target && owner.evictAtAddress(address, false)) {
        long relocated = allocator.allocate(sizeOfArea);
        if (relocated >= 0) {
          if (relocated < target) {
            writeBuffer(relocated, readBuffer(target, sizeOfArea));
            owner.moved(target, relocated);
            allocator.free(target);
            return true;
          } else {
            allocator.free(relocated);
          }
        }
      }
    }
    return false;
  }

  public boolean shrink() {
    final Lock ownerLock = owner.writeLock();
    ownerLock.lock();
    try {
      if (pages.isEmpty()) {
        return false;
      } else {
        int initialSize = pages.size();
        for (ByteData p : release(new LinkedList<ByteData>(Collections.singletonList(pages.get(pages.size() - 1))))) {
          freePage(p);
        }
        return pages.size() < initialSize;
      }
    } finally {
      ownerLock.unlock();
    }
  }

  private int getIndexForPage(ByteData p) {
    for (Entry<Integer, ByteData> e : pages.entrySet()) {
      if (e.getValue() == p) {
        return e.getKey();
      }
    }

    return -1;
  }

  public static interface Owner {
    boolean evictAtAddress(long address, boolean shrink);

    Lock writeLock();
    
    boolean isThief();

    public boolean moved(long shift, long pointer);

    public int sizeOf(long shift);
  }

  private void validatePages() {
    if (VALIDATING) {
      for (int i = 0; i < pages.size(); i++) {
        if (pages.get(i) == null) {
          List<Integer> pageIndices = new ArrayList<Integer>(pages.keySet());
          Collections.sort(pageIndices);
          throw new AssertionError("Page Indices " + pageIndices);
        }
      }
    }
  }
}
