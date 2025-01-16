/*
 * Copyright 2025 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
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
package com.terracottatech.offheapstore.storage.restartable.partial;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.offheapstore.storage.restartable.LinkedNode;
import com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.storage.restartable.RestartabilityTestUtilities;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;
import org.junit.Test;
import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.ReadWriteLockedOffHeapHashMap;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapMap;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.MemoryUnit;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

public class PartialThievingIT {

  @Test
  public void testThievingFromPartialCache() throws IOException, RestartStoreException, InterruptedException, ExecutionException {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testPutPath_");
    ByteBuffer mapId = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    ByteBuffer cacheId = ByteBuffer.wrap("cache".getBytes("US-ASCII"));

    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = RestartStoreFactory.createStore(objectMgr, directory, new Properties());
    persistence.startup().get();
    try {
      byte[] value = new byte[100*1024];
      Portability<String> keyPortability = StringPortability.INSTANCE;
      Portability<byte[]> valuePortability = ByteArrayPortability.INSTANCE;

      PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MEGABYTES.toBytes(16L), MemoryUnit.MEGABYTES.toBytes(16));

      RestartablePartialStorageEngine<ByteBuffer, String, byte[]> cacheStorageEngine = new RestartablePartialStorageEngine<>(cacheId, persistence, false, PointerSize.LONG, source, MemoryUnit.KILOBYTES.toBytes(256), keyPortability, valuePortability, 0.75f);
      ReadWriteLockedOffHeapClockCache<String, byte[]> cache = new ReadWriteLockedOffHeapClockCache<>(source, cacheStorageEngine);
      objectMgr.registerObject(new OffHeapObjectManagerStripe<>(cacheId, cache));

      OffHeapBufferStorageEngine<String, LinkedNode<byte[]>> delegateEngine = new OffHeapBufferStorageEngine<>(PointerSize.LONG, source, MemoryUnit.KILOBYTES.toBytes(256), keyPortability, new LinkedNodePortability<>(valuePortability), true, false);
      RestartableStorageEngine<?, ByteBuffer, String, byte[]> mapStorageEngine = new RestartableStorageEngine<>(mapId, persistence, delegateEngine, false);

      ReadWriteLockedOffHeapHashMap<String, byte[]> map = new ReadWriteLockedOffHeapHashMap<>(source, true, mapStorageEngine);
      objectMgr.registerObject(new OffHeapObjectManagerStripe<>(mapId, map));

      //populate cache
      for (int i = 0; cache.size() < 1000; i++) {
        cache.put(Integer.toString(i), value);
      }

      try {
        for (int i = 0; ; i++) {
          map.put(Integer.toString(i), value);
        }
      } catch (OversizeMappingException e) {
        System.out.println("Map Put: " + map.size());
      }
    } finally {
      persistence.shutdown();
    }

  }

  protected static void destroyMap(Map<?, ?> map) {
    if (map instanceof OffHeapHashMap<?, ?>) {
      ((OffHeapHashMap<?, ?>) map).destroy();
    } else if (map instanceof AbstractConcurrentOffHeapMap<?, ?>) {
      ((AbstractConcurrentOffHeapMap<?, ?>) map).destroy();
    } else {
      throw new AssertionError();
    }

  }
}
