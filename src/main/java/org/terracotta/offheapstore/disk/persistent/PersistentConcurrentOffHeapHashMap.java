/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.disk.persistent;

import java.io.IOException;
import java.io.ObjectInput;

import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.util.Factory;

/**
 *
 * @author Chris Dennis
 */
public class PersistentConcurrentOffHeapHashMap<K, V> extends AbstractPersistentConcurrentOffHeapMap<K, V> {

  public PersistentConcurrentOffHeapHashMap(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory) {
    super(new PersistentReadWriteLockedOffHeapHashMapFactory<>(tableSource, storageEngineFactory));
  }

  public PersistentConcurrentOffHeapHashMap(ObjectInput input, MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory) throws IOException {
    super(new PersistentReadWriteLockedOffHeapHashMapFactory<>(tableSource, storageEngineFactory, false), readSegmentCount(input));
  }

  public PersistentConcurrentOffHeapHashMap(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize, int concurrency) {
    super(new PersistentReadWriteLockedOffHeapHashMapFactory<>(tableSource, storageEngineFactory, (int) (tableSize / concurrency)), concurrency);
  }

  public PersistentConcurrentOffHeapHashMap(ObjectInput input, MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize, int concurrency) throws IOException {
    super(new PersistentReadWriteLockedOffHeapHashMapFactory<>(tableSource, storageEngineFactory, (int) (tableSize / concurrency), false), readSegmentCount(input));
  }
}
