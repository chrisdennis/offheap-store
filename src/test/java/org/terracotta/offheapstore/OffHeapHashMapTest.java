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

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.junit.Test;
import org.mockito.InOrder;

import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.hashcode.JdkHashCodeAlgorithm;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.BinaryStorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.CombinableMatcher.either;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 *
 * @author cdennis
 */
public class OffHeapHashMapTest {
  
  /*
   * Check that all constructors throw NPE when passed a null storage engine (really one logical test) 
   */
  @Test
  public void testNullStorageEngineCausesNpe() {
    try {
      new OffHeapHashMap<Object, Object>(mock(PageSource.class), null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }

    try {
      new OffHeapHashMap<Object, Object>(mock(PageSource.class), null, true);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }

    try {
      new OffHeapHashMap<Object, Object>(mock(PageSource.class), null, 1);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }

    try {
      new OffHeapHashMap<Object, Object>(mock(PageSource.class), true, null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }

    try {
      new OffHeapHashMap<Object, Object>(mock(PageSource.class), true, null, 1);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }

    try {
      new OffHeapHashMap<Object, Object>(mock(PageSource.class), true, null, 1, true);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }
  
  @Test
  public void testZeroInitialTableSize() {
    PageSource source = mock(PageSource.class);
    when(source.allocate(eq(16), anyBoolean(), anyBoolean(), isNull(OffHeapStorageArea.class)))
            .thenReturn(new Page(ByteBuffer.allocate(16), null));
    assertThat(new OffHeapHashMap<Object, Object>(source, mock(StorageEngine.class), 0).getTableCapacity(), is(1L));
  }
  
  @Test
  public void testPowerOfTwoInitialTableSize() {
    PageSource source = mock(PageSource.class);
    when(source.allocate(eq(4 * 16), anyBoolean(), anyBoolean(), isNull(OffHeapStorageArea.class)))
            .thenReturn(new Page(ByteBuffer.allocate(4 * 16), null));
    assertThat(new OffHeapHashMap<Object, Object>(source, mock(StorageEngine.class), 4).getTableCapacity(), is(4L));
  }

  @Test
  public void testNonPowerOfTwoInitialTableSize() {
    PageSource source = mock(PageSource.class);
    when(source.allocate(eq(8 * 16), anyBoolean(), anyBoolean(), isNull(OffHeapStorageArea.class)))
            .thenReturn(new Page(ByteBuffer.allocate(8 * 16), null));
    assertThat(new OffHeapHashMap<Object, Object>(source, mock(StorageEngine.class), 5).getTableCapacity(), is(8L));
  }
  
  @Test
  public void testStorageEngineIsBound() {
    StorageEngine<Object, Object> engine = mock(StorageEngine.class);
    OffHeapHashMap<?, ?> map = new OffHeapHashMap<Object, Object>(mock(PageSource.class), engine, false);
    verify(engine).bind(map);
  }
  
  @Test
  public void testNonBootstrappingMapDoesNotAllocateTable() {
    PageSource source = mock(PageSource.class);
    new OffHeapHashMap<Object, Object>(source, mock(StorageEngine.class), false);
    verify(source, never()).allocate(anyInt(), anyBoolean(), anyBoolean(), any(OffHeapStorageArea.class));
  }
  
  @Test
  public void testBootstrappingMapFailsWhenPageSourceFails() {
    try {
      new OffHeapHashMap<Object, Object>(mock(PageSource.class), mock(StorageEngine.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Initial table allocation failed."));
    }
  }
  
  @Test
  public void testContainsKeyOnEmptyMapShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    assertThat(map.containsKey("foo"), is(false));
    
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }
  
  @Test
  public void testContainsKeyWithNonMatchingHashCodeShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.containsKey("foo"), is(false));
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }

  @Test
  public void testContainsKeyWithMatchingHashCodeCallsStorageEngine() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("bar", "bar", "bar".hashCode(), 0)).thenReturn(42L);
    when(engine.equalsKey("bar", 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.containsKey("bar"), is(true));
  }
  
  @Test
  public void testContainsKeyWithMatchingHashCodeThatMissesReprobes() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.writeMapping(1L << Integer.SIZE, "bar", hashCode, 0)).thenReturn(43L);
    when(engine.equalsKey(1L << Integer.SIZE, 42L)).thenReturn(false);
    when(engine.equalsKey(1L << Integer.SIZE, 43L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(1L << Integer.SIZE, "bar");
    
    assertThat(map.containsKey(1L << Integer.SIZE), is(true));

    verify(engine, times(2)).equalsKey(1L << Integer.SIZE, 42L);
    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 43L);
  }

  @Test
  public void testContainsKeyTerminatesOnEmptyEntry() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
     
    map.put(1L, "bar");
    
    assertThat(map.containsKey(1L << Integer.SIZE), is(false));

    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 42L);
  }
  
  @Test
  public void testContainsKeyOnRemovedKeyMisses() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(2L, "foo");
    map.remove(1L);
    
    assertThat(map.containsKey(1L), is(false));

    verify(engine, times(1)).equalsKey(1L, 42L);
  }

  @Test
  public void testGetOnEmptyMapShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    assertThat(map.get("foo"), nullValue());
    
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }
  
  @Test
  public void testGetWithNonMatchingHashCodeShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.get("foo"), nullValue());
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }

  @Test
  public void testGetWithMatchingHashCodeCallsStorageEngine() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("bar", "bar", "bar".hashCode(), 0)).thenReturn(42L);
    when(engine.equalsKey("bar", 42L)).thenReturn(true);
    when(engine.readValue(42L)).thenReturn("bar");
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat((String) map.get("bar"), is("bar"));
  }
  
  @Test
  public void testGetWithMatchingHashCodeThatMissesReprobes() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.writeMapping(1L << Integer.SIZE, "bar", hashCode, 0)).thenReturn(43L);
    when(engine.equalsKey(1L << Integer.SIZE, 42L)).thenReturn(false);
    when(engine.equalsKey(1L << Integer.SIZE, 43L)).thenReturn(true);
    when(engine.readValue(43L)).thenReturn("bar");
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(1L << Integer.SIZE, "bar");
    
    assertThat((String) map.get(1L << Integer.SIZE), is("bar"));

    verify(engine, times(2)).equalsKey(1L << Integer.SIZE, 42L);
    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 43L);
  }

  @Test
  public void testGetTerminatesOnEmptyEntry() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    
    assertThat(map.get(1L << Integer.SIZE), nullValue());

    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 42L);
  }
  
  @Test
  public void testGetOnRemovedKeyMisses() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(2L, "foo");
    map.remove(1L);
    
    assertThat(map.get(1L), nullValue());

    verify(engine, times(1)).equalsKey(1L, 42L);
  }

  @Test
  public void testGetEncodingWithHashAndBinaryFailsWithRegularStorageEngine() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    map.put("foo", "foo");
    
    try {
      map.getEncodingForHashAndBinary(42, ByteBuffer.allocate(1));
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      assertThat(e.getMessage(), containsString("BinaryStorageEngine"));
    }
  }

  @Test
  public void testGetEncodingWithHashAndBinaryOnEmptyMapShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    assertThat(map.getEncodingForHashAndBinary(42, ByteBuffer.allocate(1)), nullValue());
    
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }
  
  @Test
  public void testGetEncodingWithHashAndBinaryWithNonMatchingHashCodeShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.getEncodingForHashAndBinary(1, ByteBuffer.allocate(1)), nullValue());
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }

  @Test
  public void testGetEncodingWithHashAndBinaryWithMatchingHashCodeCallsStorageEngine() {
    StorageEngine<String, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(engine.writeMapping("bar", "bar", "bar".hashCode(), 0)).thenReturn(42L);
    when(((BinaryStorageEngine) engine).equalsBinaryKey(ByteBuffer.allocate(1), 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.getEncodingForHashAndBinary("bar".hashCode(), ByteBuffer.allocate(1)), is(42L));
  }
  
  @Test
  public void testGetEncodingWithHashAndBinaryWithMatchingHashCodeThatMissesReprobes() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.writeMapping(1L << Integer.SIZE, "bar", hashCode, 0)).thenReturn(43L);
    when(((BinaryStorageEngine) engine).equalsBinaryKey(ByteBuffer.allocate(1), 42L)).thenReturn(false);
    when(((BinaryStorageEngine) engine).equalsBinaryKey(ByteBuffer.allocate(1), 43L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(1L << Integer.SIZE, "bar");
    
    assertThat(map.getEncodingForHashAndBinary(hashCode, ByteBuffer.allocate(1)), is(43L));

    verify((BinaryStorageEngine) engine, times(1)).equalsBinaryKey(ByteBuffer.allocateDirect(1), 42L);
    verify((BinaryStorageEngine) engine, times(1)).equalsBinaryKey(ByteBuffer.allocateDirect(1), 43L);
  }

  @Test
  public void testGetEncodingWithHashAndBinaryTerminatesOnEmptyEntry() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    
    assertThat(map.getEncodingForHashAndBinary(hashCode, ByteBuffer.allocate(1)), nullValue());

    verify((BinaryStorageEngine) engine, times(1)).equalsBinaryKey(ByteBuffer.allocate(1), 42L);
  }
  
  @Test
  public void testGetEncodingWithHashAndBinaryOnRemovedKeyMisses() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(2L, "foo");
    map.remove(1L);
    
    assertThat(map.getEncodingForHashAndBinary(hashCode, ByteBuffer.allocate(1)), nullValue());

    verify(engine, times(1)).equalsKey(1L, 42L);
  }
  
  @Test
  public void testPutWithNullKeyThrowsEarlyNpe() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    try {
      map.put(null, "value");
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
    
    verify(engine, only()).bind(map);
  }
  
  @Test
  public void testPutWritesMappingCorrectly() {
    StorageEngine<Object, Object> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<Object, Object> map = new OffHeapHashMap<Object, Object>(source, engine, 1);

    Object key = new Object();
    Object value = new Object();
    map.put(key, value, 40);
    
    verify(engine).writeMapping(same(key), same(value), eq(new JdkHashCodeAlgorithm().hash(key)), eq(40));
  }
  
  @Test
  public void testPutWithStorageEngineFailureTriggersOme() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(null);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    try {
      map.put("foo", "bar");
      fail("Expected OversizeMappingException");
    } catch (OversizeMappingException e) {
      assertThat(e.getMessage(), containsString("Storage engine failed to store"));
    }
  }
  
  @Test
  public void testPutWithInvalidMetadataTriggersIae() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    try {
      map.put("foo", "bar", OffHeapHashMap.RESERVED_STATUS_BITS);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Invalid metadata for key"));
    }
  }
  
  @Test
  public void testPutToEmptyMapWithOneSlotSuceeds() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    map.put("foo", "bar");
    
    assertThat(map.size(), is(1));
    assertThat(map.getUsedSlotCount(), is(1L));
    verify(engine).attachedMapping(42L, "foo".hashCode(), 0);
    verify(engine).invalidateCache();
  }
  
  @Test
  public void testPutToFullTableTriggersResize() {
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, mock(StorageEngine.class), 1);

    map.put("foo", "bar");
    map.put("bar", "foo");
    
    assertThat(map.getTableCapacity(), is(2L));
  }
  
  @Test
  public void testPutOverwritesRemovedSlot() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    when(engine.equalsKey("foo", 42L)).thenReturn(true);
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 2);

    map.put("foo", "bar");
    map.remove("foo");
    
    assertThat(map.getUsedSlotCount(), is(0L));
    assertThat(map.getRemovedSlotCount(), is(1L));
    
    map.put("foo", "bar");
    assertThat(map.getRemovedSlotCount(), is(0L));
  }

  @Test
  public void testPutOverwriteReusesExistingSlot() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    when(engine.writeMapping("foo", "bat", "foo".hashCode(), 0)).thenReturn(43L);
    when(engine.equalsKey("foo", 42L)).thenReturn(true);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    map.put("foo", "bar");
    map.put("foo", "bat");
  }

  @Test
  public void testPutOverwriteWithEarlyAvailableSlotOverwrites() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L << Integer.SIZE, "baz", hashCode, 0)).thenReturn(1L);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.writeMapping(1L, "bat", hashCode, 0)).thenReturn(43L);
    when(engine.equalsKey(1L << Integer.SIZE, 1L)).thenReturn(true);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    when(engine.readValue(42L)).thenReturn("bar");
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);

    map.put(1L << Integer.SIZE, "baz");
    map.put(1L, "bar");
    map.remove(1L << Integer.SIZE);

    assertThat(map.getRemovedSlotCount(), is(1L));
    assertThat(map.put(1L, "bat"), is("bar"));
    assertThat(map.getRemovedSlotCount(), is(1L));
  }

  @Test
  public void testPutNonTerminatingProbeSequenceWithEarlyAvailableSlot() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    when(engine.writeMapping("bar", "foo", "bar".hashCode(), 0)).thenReturn(43L);
    when(engine.equalsKey("foo", 42L)).thenReturn(true);
    when(engine.equalsKey("bar", 43L)).thenReturn(true);
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 2);

    map.put("foo", "bar");
    map.put("bar", "foo");
    map.remove("foo");
    map.remove("bar");
    assertThat(map.getRemovedSlotCount(), is(2L));

    map.put("foo", "bar");
    
    assertThat(map.getRemovedSlotCount(), is(1L));
  }
  
  @Test
  public void testPutReturnsToEarlyAvailableSlot() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    when(engine.writeMapping("bar", "foo", "bar".hashCode(), 0)).thenReturn(43L);
    when(engine.equalsKey("foo", 42L)).thenReturn(true);
    when(engine.equalsKey("bar", 43L)).thenReturn(true);
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 2);

    map.put("foo", "bar");
    map.put("bar", "foo");
    map.remove("bar");
    assertThat(map.getRemovedSlotCount(), is(1L));

    map.put("bar", "foo");
    
    assertThat(map.getRemovedSlotCount(), is(0L));
  }
  
  @Test
  public void testPutFreesAndRewritesMappingOnTableExpansion() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("bar", "foo", "bar".hashCode(), 0)).thenReturn(42L);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    map.put("foo", "bar");
    map.put("bar", "foo");
    
    InOrder ordered = inOrder(engine);
    ordered.verify(engine).writeMapping("bar", "foo", "bar".hashCode(), 0);
    ordered.verify(engine).freeMapping(42L, "bar".hashCode(), false);
    ordered.verify(engine).writeMapping("bar", "foo", "bar".hashCode(), 0);
  }
  
  @Test
  public void testMultipleCollidingPutsCausesReprobeExpansion() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(512, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(512), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 32);

    assertThat(map.getReprobeLength(), is(16));
    for (int i = 0; i < 16; i++) {
      map.put("foo", "foo");
    }
    assertThat(map.getReprobeLength(), is(16));
    map.put("foo", "foo");
    assertThat(map.getReprobeLength(), is(32));
  }
  
  @Test
  public void testInstallWritesMappingCorrectly() {
    StorageEngine<String, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    ByteBuffer key = ByteBuffer.allocate(1);
    ByteBuffer value = ByteBuffer.allocate(2);
    map.installMappingForHashAndEncoding(42L, key, value, 40);
    
    verify((BinaryStorageEngine) engine).writeBinaryMapping(same(key), same(value), eq(42L), eq(40));
  }
  
  @Test
  public void testInstallWithStorageEngineFailureTriggersOme() {
    StorageEngine<String, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(((BinaryStorageEngine) engine).writeBinaryMapping(ByteBuffer.allocate(1), ByteBuffer.allocate(1), 42, 0)).thenReturn(null);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    try {
      map.installMappingForHashAndEncoding(42, ByteBuffer.allocate(1), ByteBuffer.allocate(1), 0);
      fail("Expected OversizeMappingException");
    } catch (OversizeMappingException e) {
      assertThat(e.getMessage(), containsString("Storage engine failed to store"));
    }
  }
  
  @Test
  public void testInstallWithInvalidMetadataTriggersIae() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    try {
      map.installMappingForHashAndEncoding(42, ByteBuffer.allocate(1), ByteBuffer.allocate(1), OffHeapHashMap.RESERVED_STATUS_BITS);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Invalid metadata for binary key"));
    }
  }
  
  @Test
  public void testInstallToEmptyMapWithOneSlotSuceeds() {
    StorageEngine<String, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    map.installMappingForHashAndEncoding(1, ByteBuffer.allocate(1), ByteBuffer.allocate(2), 0);
    
    assertThat(map.size(), is(1));
    assertThat(map.getUsedSlotCount(), is(1L));
    verify((BinaryStorageEngine) engine).writeBinaryMapping(ByteBuffer.allocate(1), ByteBuffer.allocate(2), 1, 0);
  }
  
  @Test
  public void testInstallToFullTableTriggersResize() {
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class)), 1);

    map.installMappingForHashAndEncoding(1, ByteBuffer.allocate(0), ByteBuffer.allocate(0), 0);
    map.installMappingForHashAndEncoding(2, ByteBuffer.allocate(0), ByteBuffer.allocate(0), 0);
    
    assertThat(map.getTableCapacity(), is(2L));
  }
  
  @Test
  public void testInstallOverwritesRemovedSlot() {
    StorageEngine<String, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    when(engine.equalsKey("foo", 42L)).thenReturn(true);
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 2);

    map.put("foo", "bar");
    map.remove("foo");
    
    assertThat(map.getUsedSlotCount(), is(0L));
    assertThat(map.getRemovedSlotCount(), is(1L));
    
    map.installMappingForHashAndEncoding("foo".hashCode(), ByteBuffer.allocate(0), ByteBuffer.allocate(0), 0);
    assertThat(map.getRemovedSlotCount(), is(0L));
  }

  @Test
  public void testInstallFreesAndRewritesMappingOnTableExpansion() {
    StorageEngine<String, String> engine = mock(StorageEngine.class, withSettings().extraInterfaces(BinaryStorageEngine.class));
    when(((BinaryStorageEngine) engine).writeBinaryMapping(ByteBuffer.allocate(0), ByteBuffer.allocate(0), "bar".hashCode(), 0)).thenReturn(42L);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    map.put("foo", "bar");
    map.installMappingForHashAndEncoding("bar".hashCode(), ByteBuffer.allocate(0), ByteBuffer.allocate(0), 0);
    
    InOrder ordered = inOrder(engine);
    ordered.verify((BinaryStorageEngine) engine).writeBinaryMapping(ByteBuffer.allocate(0), ByteBuffer.allocate(0), "bar".hashCode(), 0);
    ordered.verify(engine).freeMapping(42L, "bar".hashCode(), false);
    ordered.verify((BinaryStorageEngine) engine).writeBinaryMapping(ByteBuffer.allocate(0), ByteBuffer.allocate(0), "bar".hashCode(), 0);
  }

  @Test
  public void testUpdateMetadataOnEmptyMapShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.getAndSetMetadata("foo", ~0, ~0);
    
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }
  
  @Test
  public void testUpdateMetadataWithNonMatchingHashCodeShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.getAndSetMetadata("foo", ~0, ~0), nullValue());
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }

  @Test
  public void testUpdateMetadataWithMatchingHashCodeCallsStorageEngine() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("bar", "bar", "bar".hashCode(), 0)).thenReturn(42L);
    when(engine.equalsKey("bar", 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.getAndSetMetadata("bar", ~0, ~0), is(0));
  }
  
  @Test
  public void testUpdateMetadataWithMatchingHashCodeThatMissesReprobes() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.writeMapping(1L << Integer.SIZE, "bar", hashCode, 0)).thenReturn(43L);
    when(engine.equalsKey(1L << Integer.SIZE, 42L)).thenReturn(false);
    when(engine.equalsKey(1L << Integer.SIZE, 43L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar", 0);
    map.put(1L << Integer.SIZE, "bar", 0);
    
    assertThat(map.getAndSetMetadata(1L << Integer.SIZE, ~0, 8), is(0));
    
    verify(engine, times(2)).equalsKey(1L << Integer.SIZE, 42L);
    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 43L);
  }

  @Test
  public void testUpdateMetadataTerminatesOnEmptyEntry() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    
    assertThat(map.getAndSetMetadata(1L << Integer.SIZE, ~0, ~0), nullValue());

    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 42L);
  }
  
  @Test
  public void testUpdateMetadataOnRemovedKeyMisses() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(2L, "foo");
    map.remove(1L);
    
    assertThat(map.getAndSetMetadata(1L, ~0, ~0), nullValue());

    verify(engine, times(1)).equalsKey(1L, 42L);
  }
  
  @Test
  public void testUpdateMetadataMaskVetoesWrite() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("bar", "bar", "bar".hashCode(), 0)).thenReturn(42L);
    when(engine.equalsKey("bar", 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar", 0);
    
    assertThat(map.getAndSetMetadata("bar", 0, 32), is(0));
    assertThat(map.getMetadata("bar", ~0), is(0));
  }

  @Test
  public void testUpdateMetadataWritesMetadata() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("bar", "bar", "bar".hashCode(), 4)).thenReturn(42L);
    when(engine.equalsKey("bar", 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar", 4);
    
    assertThat(map.getAndSetMetadata("bar", ~4, 32), is(0));
    assertThat(map.getMetadata("bar", ~0), is(36));
  }

  @Test
  public void testGetMetadataOnEmptyMapShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    assertThat(map.getMetadata("foo", ~0), nullValue());
    
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }
  
  @Test
  public void testGetMetadataWithNonMatchingHashCodeShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar", 8);
    
    assertThat(map.getMetadata("foo", ~0), nullValue());
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }

  @Test
  public void testGetMetadataWithMatchingHashCodeCallsStorageEngine() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("bar", "bar", "bar".hashCode(), 8)).thenReturn(42L);
    when(engine.equalsKey("bar", 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar", 8);
    
    assertThat(map.getMetadata("bar", ~0), is(8));
  }
  
  @Test
  public void testGetMetadataWithMatchingHashCodeThatMissesReprobes() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.writeMapping(1L << Integer.SIZE, "bar", hashCode, 8)).thenReturn(43L);
    when(engine.equalsKey(1L << Integer.SIZE, 42L)).thenReturn(false);
    when(engine.equalsKey(1L << Integer.SIZE, 43L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar", 0);
    map.put(1L << Integer.SIZE, "bar", 8);
    
    assertThat(map.getMetadata(1L << Integer.SIZE, ~0), is(8));

    verify(engine, times(2)).equalsKey(1L << Integer.SIZE, 42L);
    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 43L);
  }

  @Test
  public void testGetMetadataTerminatesOnEmptyEntry() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 8)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar", 8);
    
    assertThat(map.getMetadata(1L << Integer.SIZE, ~0), nullValue());

    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 42L);
  }
  
  @Test
  public void testGetMetadataOnRemovedKeyMisses() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 8)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar", 8);
    map.put(2L, "foo");
    map.remove(1L);
    
    assertThat(map.getMetadata(1L, ~0), nullValue());

    verify(engine, times(1)).equalsKey(1L, 42L);
  }

  @Test
  public void testFillWithNullKeyThrowsEarlyNpe() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    try {
      map.fill(null, "value");
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
    
    verify(engine, only()).bind(map);
  }
  
  @Test
  public void testFillWritesMappingCorrectly() {
    StorageEngine<Object, Object> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<Object, Object> map = new OffHeapHashMap<Object, Object>(source, engine, 1);

    Object key = new Object();
    Object value = new Object();
    map.fill(key, value, 40);
    
    verify(engine).writeMapping(same(key), same(value), eq(new JdkHashCodeAlgorithm().hash(key)), eq(40));
  }
  
  @Test
  public void testFillWithStorageEngineFailureFailsWithoutException() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(null);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    assertThat(map.fill("foo", "bar"), nullValue());
  }
  
  @Test
  public void testFillWithInvalidMetadataTriggersIae() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    try {
      map.fill("foo", "bar", OffHeapHashMap.RESERVED_STATUS_BITS);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Invalid metadata for key"));
    }
  }
  
  @Test
  public void testFillToEmptyMapWithOneSlotSuceeds() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    map.put("foo", "bar");
    
    assertThat(map.size(), is(1));
    assertThat(map.getUsedSlotCount(), is(1L));
    verify(engine).attachedMapping(42L, "foo".hashCode(), 0);
    verify(engine).invalidateCache();
  }
  
  @Test
  public void testFillToFullTableTriggersResize() {
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, mock(StorageEngine.class), 1);

    map.put("foo", "bar");
    map.put("bar", "foo");
    
    assertThat(map.getTableCapacity(), is(2L));
  }
  
  @Test
  public void testFillWithFailedTableResizeFailsWithoutException() {
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    when(source.allocate(32, false, false, null)).thenReturn(null);
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, mock(StorageEngine.class), 1);

    map.put("foo", "bar");
    assertThat(map.fill("bar", "foo"), nullValue());
    assertThat(map.getTableCapacity(), is(1L));
  }
  
  @Test
  public void testFillOverwritesRemovedSlot() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    when(engine.equalsKey("foo", 42L)).thenReturn(true);
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 2);

    map.put("foo", "bar");
    map.remove("foo");
    
    assertThat(map.getUsedSlotCount(), is(0L));
    assertThat(map.getRemovedSlotCount(), is(1L));
    
    map.fill("foo", "bar");
    assertThat(map.getRemovedSlotCount(), is(0L));
  }

  @Test
  public void testFillOverwriteReusesExistingSlot() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    when(engine.writeMapping("foo", "bat", "foo".hashCode(), 0)).thenReturn(43L);
    when(engine.equalsKey("foo", 42L)).thenReturn(true);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    map.put("foo", "bar");
    map.fill("foo", "bat");
  }

  @Test
  public void testFillOverwriteWithEarlyAvailableSlotOverwrites() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L << Integer.SIZE, "baz", hashCode, 0)).thenReturn(1L);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.writeMapping(1L, "bat", hashCode, 0)).thenReturn(43L);
    when(engine.equalsKey(1L << Integer.SIZE, 1L)).thenReturn(true);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    when(engine.readValue(42L)).thenReturn("bar");
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);

    map.put(1L << Integer.SIZE, "baz");
    map.put(1L, "bar");
    map.remove(1L << Integer.SIZE);

    assertThat(map.getRemovedSlotCount(), is(1L));
    assertThat(map.fill(1L, "bat"), is("bar"));
    assertThat(map.getRemovedSlotCount(), is(1L));
  }

  @Test
  public void testFillNonTerminatingProbeSequenceWithEarlyAvailableSlot() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    when(engine.writeMapping("bar", "foo", "bar".hashCode(), 0)).thenReturn(43L);
    when(engine.equalsKey("foo", 42L)).thenReturn(true);
    when(engine.equalsKey("bar", 43L)).thenReturn(true);
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 2);

    map.put("foo", "bar");
    map.put("bar", "foo");
    map.remove("foo");
    map.remove("bar");
    assertThat(map.getRemovedSlotCount(), is(2L));

    map.fill("foo", "bar");
    
    assertThat(map.getRemovedSlotCount(), is(1L));
  }
  
  @Test
  public void testFillReturnsToEarlyAvailableSlot() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("foo", "bar", "foo".hashCode(), 0)).thenReturn(42L);
    when(engine.writeMapping("bar", "foo", "bar".hashCode(), 0)).thenReturn(43L);
    when(engine.equalsKey("foo", 42L)).thenReturn(true);
    when(engine.equalsKey("bar", 43L)).thenReturn(true);
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 2);

    map.put("foo", "bar");
    map.put("bar", "foo");
    map.remove("bar");
    assertThat(map.getRemovedSlotCount(), is(1L));

    map.fill("bar", "foo");
    
    assertThat(map.getRemovedSlotCount(), is(0L));
  }
  
  @Test
  public void testFillWritesAfterTableExpansion() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("bar", "foo", "bar".hashCode(), 0)).thenReturn(42L);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);

    map.put("foo", "bar");
    map.fill("bar", "foo");

    InOrder ordered = inOrder(engine, source);
    ordered.verify(source).allocate(32, false, false, null);
    ordered.verify(engine).writeMapping("bar", "foo", "bar".hashCode(), 0);
    verify(engine, never()).freeMapping(42L, "bar".hashCode(), false);
  }

  @Test
  public void testRemoveOnEmptyMapShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    assertThat(map.remove("foo"), nullValue());
    
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }
  
  @Test
  public void testRemoveWithNonMatchingHashCodeShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.remove("foo"), nullValue());
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }

  @Test
  public void testRemoveWithMatchingHashCodeCallsStorageEngine() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("bar", "bar", "bar".hashCode(), 0)).thenReturn(42L);
    when(engine.equalsKey("bar", 42L)).thenReturn(true);
    when(engine.readValue(42L)).thenReturn("bar");
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.remove("bar"), is("bar"));
    verify(engine).freeMapping(42L, "bar".hashCode(), true);
    assertThat(map.size(), is(0));
  }
  
  @Test
  public void testRemoveWithMatchingHashCodeThatMissesReprobes() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.writeMapping(1L << Integer.SIZE, "bat", hashCode, 0)).thenReturn(43L);
    when(engine.equalsKey(1L << Integer.SIZE, 42L)).thenReturn(false);
    when(engine.equalsKey(1L << Integer.SIZE, 43L)).thenReturn(true);
    when(engine.readValue(43L)).thenReturn("bat");
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(1L << Integer.SIZE, "bat");
    
    assertThat(map.remove(1L << Integer.SIZE), is("bat"));

    verify(engine, times(2)).equalsKey(1L << Integer.SIZE, 42L);
    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 43L);
    verify(engine).freeMapping(43L, hashCode, true);
    assertThat(map.size(), is(1));
  }

  @Test
  public void testRemoveTerminatesOnEmptyEntry() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    
    assertThat(map.remove(1L << Integer.SIZE), nullValue());

    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 42L);
  }
  
  @Test
  public void testRemoveOnRemovedKeyMisses() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(2L, "foo");
    map.remove(1L);
    
    assertThat(map.remove(1L), nullValue());

    verify(engine, times(1)).equalsKey(1L, 42L);
  }

  @Test
  public void testRemoveNoReturnOnEmptyMapShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    assertThat(map.removeNoReturn("foo"), is(false));
    
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }
  
  @Test
  public void testRemoveNoReturnWithNonMatchingHashCodeShortCircuits() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.removeNoReturn("foo"), is(false));
    verify(engine, never()).equalsKey(any(), anyLong());
    verify(engine, never()).readKey(anyLong(), anyInt());
  }

  @Test
  public void testRemoveNoReturnWithMatchingHashCodeCallsStorageEngine() {
    StorageEngine<String, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping("bar", "bar", "bar".hashCode(), 0)).thenReturn(42L);
    when(engine.equalsKey("bar", 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    
    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(source, engine, 1);
    
    map.put("bar", "bar");
    
    assertThat(map.removeNoReturn("bar"), is(true));
    verify(engine).freeMapping(42L, "bar".hashCode(), true);
    verify(engine, never()).readValue(42L);
    assertThat(map.size(), is(0));
  }
  
  @Test
  public void testRemoveNoReturnWithMatchingHashCodeThatMissesReprobes() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.writeMapping(1L << Integer.SIZE, "bat", hashCode, 0)).thenReturn(43L);
    when(engine.equalsKey(1L << Integer.SIZE, 42L)).thenReturn(false);
    when(engine.equalsKey(1L << Integer.SIZE, 43L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(1L << Integer.SIZE, "bat");
    
    assertThat(map.removeNoReturn(1L << Integer.SIZE), is(true));

    verify(engine, times(2)).equalsKey(1L << Integer.SIZE, 42L);
    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 43L);
    verify(engine).freeMapping(43L, hashCode, true);
    verify(engine, never()).readValue(43L);
    assertThat(map.size(), is(1));
  }

  @Test
  public void testRemoveNoReturnTerminatesOnEmptyEntry() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    
    assertThat(map.removeNoReturn(1L << Integer.SIZE), is(false));

    verify(engine, times(1)).equalsKey(1L << Integer.SIZE, 42L);
  }
  
  @Test
  public void testRemoveNoReturnOnRemovedKeyMisses() {
    assertThat(Long.valueOf(1L).hashCode(), is(Long.valueOf(1L << Integer.SIZE).hashCode()));
    int hashCode = Long.valueOf(1L).hashCode();
    
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    when(engine.writeMapping(1L, "bar", hashCode, 0)).thenReturn(42L);
    when(engine.equalsKey(1L, 42L)).thenReturn(true);
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 2);
    
    map.put(1L, "bar");
    map.put(2L, "foo");
    map.remove(1L);
    
    assertThat(map.removeNoReturn(1L), is(false));

    verify(engine, times(1)).equalsKey(1L, 42L);
  }
  
  @Test
  public void testClearOnDestroyedTableIsNoOp() {
    StorageEngine<Object, Object> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    OffHeapHashMap<Object, Object> map = new OffHeapHashMap<Object, Object>(source, engine, 1);
    map.destroy();
    
    map.clear();
    verify(engine, never()).clear();
    verify(source, times(1)).allocate(anyInt(), anyBoolean(), anyBoolean(), any(OffHeapStorageArea.class));
  }
  
  @Test
  public void testClearClearsStorageEngine() {
    StorageEngine<Object, Object> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    OffHeapHashMap<Object, Object> map = new OffHeapHashMap<Object, Object>(source, engine, 1);
    
    map.clear();
    verify(engine).clear();
  }

  @Test
  public void testClearFromLargeSizeReallocates() {
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    when(source.allocate(64, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(64), null));
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 1);

    map.put(0L, "foo");
    map.put(1L, "foo");
    map.put(2L, "foo");
    
    map.clear();
    
    InOrder ordered = inOrder(source);
    ordered.verify(source).allocate(16, false, false, null);
    ordered.verify(source).allocate(32, false, false, null);
    ordered.verify(source).allocate(64, false, false, null);
    ordered.verify(source).allocate(16, false, false, null);
  }

  @Test
  public void testClearFromLargeSizeSucceedsDespiteTableAllocFailure() {
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    when(source.allocate(64, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(64), null));
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 1);

    map.put(0L, "foo");
    map.put(1L, "foo");
    map.put(2L, "foo");
    when(source.allocate(16, false, false, null)).thenReturn(null);
    
    map.clear();
    
    assertThat(map.get(0L), nullValue());
  }
  
  @Test
  public void testClearZerosTable() {
    StorageEngine<Object, Object> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    ByteBuffer buffer = ByteBuffer.allocate(16);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(buffer, null));
    OffHeapHashMap<Object, Object> map = new OffHeapHashMap<Object, Object>(source, engine, 1);
    
    buffer.putLong(0, ~0);
    buffer.putLong(8, ~0);
    
    map.clear();
    
    assertThat(buffer.getLong(0), is(0L));
    assertThat(buffer.getLong(8), is(0L));
  }
  
  @Test
  public void testClearZerosPendingTables() {
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    ByteBuffer a = ByteBuffer.allocate(16);
    ByteBuffer b = ByteBuffer.allocate(32);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(a, null));
    when(source.allocate(32, false, false, null)).thenReturn(new Page(b, null));
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 1);

    Iterator<Long> it = map.keySet().iterator();

    map.put(0L, "foo");
    map.put(1L, "foo");

    a.putLong(0, ~0);
    a.putLong(8, ~0);
    
    map.clear();
    
    assertThat(a.getLong(0), is(0L));
    assertThat(a.getLong(8), is(0L));
    assertThat(it.hasNext(), either(is(true)).or(is(false)));
  }
  
  @Test
  public void testClearOfBigTable() {
    StorageEngine<Long, String> engine = mock(StorageEngine.class);
    PageSource source = mock(PageSource.class);
    when(source.allocate(16, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(16), null));
    when(source.allocate(32, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(32), null));
    when(source.allocate(64, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(64), null));
    when(source.allocate(128, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(128), null));
    when(source.allocate(256, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(256), null));
    when(source.allocate(512, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(512), null));
    when(source.allocate(1024, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(1024), null));

    ByteBuffer buffer = ByteBuffer.allocate(2048);
    when(source.allocate(2048, false, false, null)).thenReturn(new Page(buffer, null));
    OffHeapHashMap<Long, String> map = new OffHeapHashMap<Long, String>(source, engine, 1);
    
    for (long i = 0; i < 64; i++) {
      map.put(i, "foo");
    }
    
    map.clear();
    
    for (ByteBuffer dup = (ByteBuffer) buffer.asReadOnlyBuffer().clear(); dup.hasRemaining(); assertThat(dup.get(), is((byte) 0)));
  }
  
  @Test
  public void testRemoveThroughThresholdTriggersTableShrink() {
    StorageEngine<Integer, String> engine = mock(StorageEngine.class);
    for (int i = 0; i < 10; i++) {
      when(engine.writeMapping(i, "foo", i, 0)).thenReturn((long) i);
      when(engine.equalsKey(i, i)).thenReturn(true);
      when(engine.readValue(i)).thenReturn("foo");
    }
    
    PageSource source = mock(PageSource.class);
    when(source.allocate(256, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(256), null));
    when(source.allocate(128, false, false, null)).thenReturn(new Page(ByteBuffer.allocate(128), null));
    OffHeapHashMap<Integer, String> map = new OffHeapHashMap<Integer, String>(source, engine, 16);
    
    for (int i = 0; i < 10; i++) {
      map.put(i, "foo");
    }
    for (int i = 0; i < 6; i++) {
      map.remove(i);
    }

    assertThat(map.getTableCapacity(), is(16L));
    map.remove(6);
    assertThat(map.getTableCapacity(), is(8L));
  }
}
