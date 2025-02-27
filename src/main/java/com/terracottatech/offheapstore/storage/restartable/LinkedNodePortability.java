/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
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
package com.terracottatech.offheapstore.storage.restartable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.terracotta.offheapstore.disk.persistent.PersistentPortability;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.WriteBackPortability;
import org.terracotta.offheapstore.storage.portability.WriteContext;

public class LinkedNodePortability<T> implements WriteBackPortability<LinkedNode<T>>, PersistentPortability<LinkedNode<T>>{

  public static final int LSN_OFFSET = 0;
  public static final int PREVIOUS_OFFSET = 8;
  public static final int NEXT_OFFSET = 16;
  public static final int VALUE_OFFSET = 24;
  
  private static final ByteBuffer EMPTY_HEADER;
  static {
    ByteBuffer emptyHeader = ByteBuffer.allocateDirect(VALUE_OFFSET);
    emptyHeader.putLong(LSN_OFFSET, -1);
    emptyHeader.putLong(PREVIOUS_OFFSET, RestartableStorageEngine.NULL_ENCODING);
    emptyHeader.putLong(NEXT_OFFSET, RestartableStorageEngine.NULL_ENCODING);
    EMPTY_HEADER = emptyHeader;
  }
  
  private final Portability<? super T> valuePortability;
  
  public LinkedNodePortability(Portability<? super T> valuePortability) {
    this.valuePortability = valuePortability;
  }

  @Override
  public ByteBuffer encode(LinkedNode<T> object) {
    ByteBuffer encodedValue = valuePortability.encode(object.getValue());
    ByteBuffer encodedNode = ByteBuffer.allocate(encodedValue.remaining() + VALUE_OFFSET);
    encodedNode.putLong(LSN_OFFSET, object.getLsn());
    encodedNode.putLong(PREVIOUS_OFFSET, object.getPrevious());
    encodedNode.putLong(NEXT_OFFSET, object.getNext());
    encodedNode.position(VALUE_OFFSET);
    encodedNode.put(encodedValue);
    encodedNode.flip();
    return encodedNode;
  }

  @Override
  public LinkedNode<T> decode(ByteBuffer buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object object, ByteBuffer buffer) {
    ByteBuffer valueBuffer = (ByteBuffer) buffer.position(buffer.position() + VALUE_OFFSET);
    if (object instanceof LinkedNode) {
      return valuePortability.equals(((LinkedNode<?>) object).getValue(), valueBuffer);
    } else {
      return valuePortability.equals(object, valueBuffer);
    }
  }

  @Override
  public LinkedNode<T> decode(ByteBuffer buffer, WriteContext context) {
    return new AttachedLinkedNode<T>(buffer, valuePortability, context);
  }

  @Override
  public void flush() throws IOException {
    throw new AssertionError();
  }

  @Override
  public void close() throws IOException {
    throw new AssertionError();
  }

  @Override
  public void persist(ObjectOutput output) throws IOException {
    throw new AssertionError();
  }

  @Override
  public void bootstrap(ObjectInput input) throws IOException {
    throw new AssertionError();
  }

  public static ByteBuffer emptyHeader() {
    return EMPTY_HEADER.duplicate();
  }
}
