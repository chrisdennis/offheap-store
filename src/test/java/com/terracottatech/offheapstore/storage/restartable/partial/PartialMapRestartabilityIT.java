package com.terracottatech.offheapstore.storage.restartable.partial;

import java.nio.ByteBuffer;
import java.util.Map;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.object.RegisterableObjectManager;
import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.ReadWriteLockedOffHeapHashMap;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.restartable.AbstractRestartabilityIT;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import org.terracotta.offheapstore.util.MemoryUnit;

public class PartialMapRestartabilityIT extends AbstractRestartabilityIT {

  @Override
  protected <K, V> Map<K, V> createRestartableMap(long size, MemoryUnit unit, ByteBuffer id,
                                                  RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence,
                                                  RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr,
                                                  Portability<? super K> keyPortability,
                                                  Portability<? super V> valuePortability,
                                                  boolean synchronous) {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), unit.toBytes(size), MemoryUnit.MEGABYTES.toBytes(1));
    RestartableMinimalStorageEngine<ByteBuffer, K, V> storageEngine = new RestartablePartialStorageEngine<ByteBuffer, K, V>(id, persistence, synchronous, getPointerSize(), source, MemoryUnit.KILOBYTES.toBytes(1), keyPortability, valuePortability, 0.75f);
    OffHeapHashMap<K, V> map = new ReadWriteLockedOffHeapHashMap<K, V>(new UnlimitedPageSource(new OffHeapBufferSource()), storageEngine);
    objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
    return map;
  }
}