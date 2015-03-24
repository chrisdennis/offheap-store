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

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.terracotta.offheapstore.data.ByteData;
import org.terracotta.offheapstore.data.Data;
import org.terracotta.offheapstore.data.Source;

/**
 * A {@link PhantomReference} based limited byte buffer source.
 * <p>
 * This buffer source tracks 'freeing' of allocated byte buffers using phantom
 * references to the allocated buffers and an associated reference queue.  An
 * {@link AtomicLong} is then used to track number of available bytes for
 * allocation.
 * 
 * @author Chris Dennis
 */
public class PhantomReferenceLimitedPageSource implements Source<ByteData> {

  private final ReferenceQueue<ByteBuffer> allocatedBuffers = new ReferenceQueue<ByteBuffer>();
  private final Map<PhantomReference<ByteBuffer>, Integer> bufferSizes = new ConcurrentHashMap<PhantomReference<ByteBuffer>, Integer>();

  private final AtomicLong max;

  /**
   * Create a source that will allocate at most {@code max} bytes.
   * 
   * @param max the maximum total size of all available buffers
   */
  public PhantomReferenceLimitedPageSource(long max) {
      this.max = new AtomicLong(max);
  }

  /**
   * Allocates a byte buffer of the given size.
   * <p>
   * This {@code BufferSource} places no restrictions on the requested size of
   * the buffer.
   */
  @Override
  public ByteData allocate(long size, boolean thief, boolean victim, OffHeapStorageArea owner) {
    if (size > Integer.MAX_VALUE) {
      return null;
    } else {
      while (true) {
        processQueue();
        long now = max.get();
        if (now < size) {
          ByteBuffer buffer;
          try {
            buffer = ByteBuffer.allocateDirect((int) size);
          } catch (OutOfMemoryError e) {
            return null;
          }
          bufferSizes.put(new PhantomReference<ByteBuffer>(buffer, allocatedBuffers), Integer.valueOf((int) size));
          return Data.wrap(buffer);
        }
      }
    }
  }

  /**
   * Frees the supplied buffer.
   * <p>
   * This implementation is a no-op, no validation of the supplied buffer is
   * attempted, as freeing of allocated buffers is monitored via phantom
   * references.
   */
  @Override
  public void free(ByteData buffer) {
    //no-op
  }

  private void processQueue() {
      while (true) {
          Reference<?> ref = allocatedBuffers.poll();
          
          if (ref == null) {
              return;
          } else {
              Integer size = bufferSizes.remove(ref);
              max.addAndGet(size.longValue());
          }
      }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PhantomReferenceLimitedBufferSource\n");
    Collection<Integer> buffers = bufferSizes.values();
    sb.append("Bytes Available   : ").append(max.get()).append('\n');
    sb.append("Buffers Allocated : (count=").append(buffers.size()).append(") ").append(buffers);
    return sb.toString();
  }
}
