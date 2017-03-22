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

import org.junit.Assert;
import org.junit.Test;
import org.terracotta.offheapstore.WriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.it.util.PointerSizeParameterizedTest;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class OffHeapStorageAreaTest extends PointerSizeParameterizedTest {

  @Test
  public void testNonStraddlingReadBuffersReturnsSingleBuffer() {
    PageSource pageSource = new UnlimitedPageSource(new HeapBufferSource());
    OffHeapStorageArea osa = new OffHeapStorageArea(getPointerSize(), null, pageSource, 1024, false, false);

    assertThat(osa.readBuffers(osa.allocate(64), 64), arrayWithSize(1));
  }

  @Test
  public void testNonStraddlingReadBuffersReturnsReadOnlyBuffer() {
    PageSource pageSource = new UnlimitedPageSource(new HeapBufferSource());
    OffHeapStorageArea osa = new OffHeapStorageArea(getPointerSize(), null, pageSource, 1024, false, false);

    assertThat(osa.readBuffers(osa.allocate(64), 64)[0].isReadOnly(), is(true));
  }

  @Test
  public void testStraddlingReadBuffersReturnsMultipleBuffers() {
    PageSource pageSource = new UnlimitedPageSource(new HeapBufferSource());
    OffHeapStorageArea osa = new OffHeapStorageArea(getPointerSize(), null, pageSource, 128, false, false);

    assertThat(osa.readBuffers(osa.allocate(256), 256), arrayWithSize(3));
  }

  @Test
  public void testStraddlingReadBuffersReturnsReadOnlyBuffers() {
    PageSource pageSource = new UnlimitedPageSource(new HeapBufferSource());
    OffHeapStorageArea osa = new OffHeapStorageArea(getPointerSize(), null, pageSource, 128, false, false);

    for (ByteBuffer buffer : osa.readBuffers(osa.allocate(256), 256)) {
      assertThat(buffer.isReadOnly(), is(true));
    }
  }

  @Test
  public void testReadBuffersReturnsCorrectData() {
    PageSource pageSource = new UnlimitedPageSource(new HeapBufferSource());
    OffHeapStorageArea osa = new OffHeapStorageArea(getPointerSize(), null, pageSource, 128, false, false);

    long base = osa.allocate(1024);
    for (int i = 0; i < 1024; i++) {
      osa.writeByte(base + i, (byte) i);
    }

    for (int i = 1; i < 512; i <<= 1) {
      for (int o = -1; o <= 1; o++) {
        ByteBuffer[] buffers = osa.readBuffers(base, i + o);

        int j = 0;
        for (ByteBuffer buffer : buffers) {
          while (buffer.hasRemaining()) {
            assertThat(buffer.get(), is((byte) j));
            j++;
          }
        }
        assertThat(j, is(i + o));
      }
    }
  }

  @Test
  public void testNonStraddlingReadBufferReturnsReadOnlyBuffer() {
    PageSource pageSource = new UnlimitedPageSource(new HeapBufferSource());
    OffHeapStorageArea osa = new OffHeapStorageArea(getPointerSize(), null, pageSource, 1024, false, false);

    assertThat(osa.readBuffer(osa.allocate(64), 64).isReadOnly(), is(true));
  }

  @Test
  public void testStraddlingReadBufferReturnsReadOnlyBuffer() {
    PageSource pageSource = new UnlimitedPageSource(new HeapBufferSource());
    OffHeapStorageArea osa = new OffHeapStorageArea(getPointerSize(), null, pageSource, 128, false, false);

    assertThat(osa.readBuffer(osa.allocate(256), 256).isReadOnly(), is(true));
  }

  @Test
  public void testReadBufferReturnsCorrectData() {
    PageSource pageSource = new UnlimitedPageSource(new HeapBufferSource());
    OffHeapStorageArea osa = new OffHeapStorageArea(getPointerSize(), null, pageSource, 128, false, false);

    long base = osa.allocate(1024);
    for (int i = 0; i < 1024; i++) {
      osa.writeByte(base + i, (byte) i);
    }

    for (int i = 1; i < 512; i <<= 1) {
      for (int o = -1; o <= 1; o++) {
        ByteBuffer buffer = osa.readBuffer(base, i + o);

        int j = 0;
        while (buffer.hasRemaining()) {
          assertThat(buffer.get(), is((byte) j));
          j++;
        }
        assertThat(j, is(i + o));
      }
    }
  }

  @Test
  public void testRecoveryOfPages() {
    GettablePageSource source = new GettablePageSource();

    Map<Integer, byte[]> test = new WriteLockedOffHeapClockCache<Integer, byte[]>(new UnlimitedPageSource(new OffHeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 1024, ByteArrayPortability.INSTANCE)));

    int put = 0;
    while (source.allocated.size() < 2) {
      test.put(put++, new byte[128]);
    }

    Assert.assertEquals(put, test.size());
    source.release();
    Assert.assertEquals(put - 1, test.size());

    Assert.assertNull(test.get(put - 1));
    for (int i = 0; i < put - 1; i++) {
      Assert.assertNotNull(test.get(i));
    }

    source.release();
    Assert.assertTrue(test.isEmpty());

    for (int i = 0; i < put; i++) {
      Assert.assertNull(test.get(i));
    }
  }

  @Test
  public void testVariablePageSize() {
    PageSource source = new UnlimitedPageSource(new HeapBufferSource());

    OffHeapStorageArea storage = new OffHeapStorageArea(getPointerSize(), null, source, 1, 1024, false, false);

    Map<Integer, Long> locations = new HashMap<Integer, Long>();

    for (int i = 0; i < 2048; i++) {
      long pointer = storage.allocate(Integer.SIZE / Byte.SIZE);
      storage.writeInt(pointer, i);
      locations.put(i, pointer);
    }

    System.err.println(storage);

    for (int i = 0; i < 2048; i++) {
      int pointer = locations.get(i).intValue();
      Assert.assertEquals(i, storage.readInt(pointer));
    }

    for (Long pointer : locations.values()) {
      storage.free(pointer);
    }
  }

//  @Test
//  public void testVariablePageSizeAddressLogic() {
//    PageSource source = new UnlimitedPageSource(new HeapBufferSource());
//
//    OffHeapStorageArea storage = new OffHeapStorageArea(null, source, 1, 1024, false, false);
//
//    for (int i = 0, address = 0; i < 100; i++) {
//      int size = storage.pageSizeFor(i);
//      int base = storage.addressForPage(i);
//      Assert.assertTrue(size <= 1024);
//      Assert.assertEquals(address, base);
//      address += size;
////      System.err.println("Page : " + i);
////      System.err.println("Size : " + size);
////      System.err.println("Base : " + base);
////      System.err.println();
//    }
//
//    for (int i = 0; i < 10240; i++) {
//      int page = storage.pageIndexFor(i);
//      int pageAddress = storage.pageAddressFor(i);
//      Assert.assertEquals(i, pageAddress + storage.addressForPage(page));
////      System.err.println("Address      : " + i);
////      System.err.println("Page Index   : " + page);
////      System.err.println("Page Address : " + pageAddress);
////      System.err.println();
//    }
//  }

  static class GettablePageSource implements PageSource {

    final Random rndm = new Random();
    final PageSource delegate = new UnlimitedPageSource(new OffHeapBufferSource());
    final List<Page> allocated = new LinkedList<Page>();

    @Override
    public Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
      Page p = delegate.allocate(size, thief, victim, owner);
      allocated.add(p);
      return p;
    }

    @Override
    public void free(Page page) {
      delegate.free(page);
      allocated.remove(page);
    }

    public void release() {
      Page p = allocated.get(rndm.nextInt(allocated.size()));
      p.binding().release(new LinkedList<Page>(Collections.singleton(p)));
      free(p);
    }
  }
}
