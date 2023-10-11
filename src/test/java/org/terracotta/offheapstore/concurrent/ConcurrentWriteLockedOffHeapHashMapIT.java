/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.concurrent;

import org.terracotta.offheapstore.concurrent.ConcurrentWriteLockedOffHeapHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.terracotta.offheapstore.AbstractConcurrentOffHeapMapIT;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.util.Generator;
import static org.terracotta.offheapstore.util.Generator.BAD_GENERATOR;
import static org.terracotta.offheapstore.util.Generator.GOOD_GENERATOR;
import org.terracotta.offheapstore.util.Generator.SpecialInteger;
import org.terracotta.offheapstore.util.ParallelParameterized;
import java.util.Arrays;
import java.util.Collection;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;
import org.junit.runner.RunWith;

@RunWith(ParallelParameterized.class)
public class ConcurrentWriteLockedOffHeapHashMapIT extends AbstractConcurrentOffHeapMapIT {

  @ParallelParameterized.Parameters(name = "generator={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[] {GOOD_GENERATOR}, new Object[] {BAD_GENERATOR});
  }

  public ConcurrentWriteLockedOffHeapHashMapIT(Generator generator) {
    super(generator);
  }

  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createMap(Generator generator) {
    return new ConcurrentWriteLockedOffHeapHashMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), generator.factory(), 1, 16);
  }

  @Override
  protected Map<Integer, byte[]> createOffHeapBufferMap(PageSource source) {
    assumeThat(generator, is(GOOD_GENERATOR));
    return new ConcurrentWriteLockedOffHeapHashMap<>(source, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine
      .createFactory(source, 1024, ByteArrayPortability.INSTANCE)));
  }
}
