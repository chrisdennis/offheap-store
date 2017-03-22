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
package org.terracotta.offheapstore.it.util;

import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.Portability;

import org.junit.runner.RunWith;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.offheapstore.util.MemoryUnit;

/**
 *
 * @author cdennis
 */
@RunWith(ParallelParameterized.class)
public abstract class StorageEngineDependentTest {

  public final TestMode testMode;

  public StorageEngineDependentTest(TestMode mode) {
    this.testMode = mode;
  }

  public PageSource createPageSource(long size, MemoryUnit unit) {
    return testMode.createPageSource(size, unit);
  }

  public <K, V> Factory<? extends StorageEngine<K, V>> createFactory(PageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability) {
    return createFactory(source, keyPortability, valuePortability, false, false);
  }

  public <K, V> Factory<? extends StorageEngine<K, V>> createFactory(PageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
    return testMode.createStorageEngineFactory(source, keyPortability, valuePortability, thief, victim);
  }

  public <K,V> StorageEngine<K, V> create(PageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability) {
    return create(source, keyPortability, valuePortability, false, false);
  }

  public <K,V> StorageEngine<K, V> create(PageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
    return testMode.createStorageEngine(source, keyPortability, valuePortability, thief, victim);
  }

  public interface TestMode {
    public abstract PageSource createPageSource(long size, MemoryUnit unit);
    public abstract <K, V> StorageEngine<K, V> createStorageEngine(PageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim);
    public abstract <K, V> Factory<? extends StorageEngine<K, V>> createStorageEngineFactory(PageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim);
  }
}
