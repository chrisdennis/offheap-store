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
package org.terracotta.offheapstore.disk.paging;

import org.terracotta.offheapstore.disk.paging.MappedPage;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.disk.AbstractDiskTest;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.size;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 *
 * @author Chris Dennis
 */
public class MappedPageSourceIT extends AbstractDiskTest {

  @Test
  public void testMappedBufferIsConnected() throws IOException {
    MappedPageSource source = new MappedPageSource(dataFile.toPath());
    try {
      MappedPage page = source.allocate(128, false, false, null);
      byte[] data = new byte[page.size()];
      Arrays.fill(data, (byte) 0xff);
      page.asByteBuffer().put(data);
      page.asByteBuffer().force();
      source.free(page);
    } finally {
      source.close();
    }

    assertThat(size(source.getFile()), is(128L));
    for (byte b : readAllBytes(source.getFile())) {
      Assert.assertEquals((byte) 0xff, b);
    }
  }

  @Test
  public void testCreateLotsOfMappedBuffers() throws IOException {
    final int size = 1024 * 1024;
    final int count = 16;

    MappedPageSource source = new MappedPageSource(dataFile.toPath());
    try {
      byte[] data = new byte[1024];
      Arrays.fill(data, (byte) 0xff);

      List<MappedPage> pages = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        pages.add(source.allocate(size, false, false, null));
      }

      for (MappedPage p : pages) {
        MappedByteBuffer b = p.asByteBuffer();
        while (b.hasRemaining()) {
          b.put(data);
        }
        b.force();
      }
    } finally {
      source.close();
    }

    assertThat(size(source.getFile()), is((long) size * count));
  }

  @Test
  public void testStraightToFileMap() throws IOException {
    final int size = 1024;

    MappedPageSource source = new MappedPageSource(dataFile.toPath());
    try {
      OffHeapHashMap<Integer, Integer> map = new OffHeapHashMap<>(source, new SplitStorageEngine<>(new IntegerStorageEngine(), new IntegerStorageEngine()));

      for (int i = 0; i < size; i++) {
        map.put(i, i);
      }

      for (int i = 0; i < size; i++) {
        Assert.assertTrue(map.containsKey(i));
        Assert.assertEquals(i, map.get(i).intValue());
      }

      for (int i = 0; i < size; i++) {
        map.remove(i);
      }

      Assert.assertTrue(map.isEmpty());
      Assert.assertEquals(0, map.getDataOccupiedMemory());
      Assert.assertEquals(0, map.getOccupiedMemory());
    } finally {
      source.close();
    }
  }
}
