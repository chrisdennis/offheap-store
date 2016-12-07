package com.terracottatech.offheapstore.storage.restartable;

import static com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability.LSN_OFFSET;
import static com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability.NEXT_OFFSET;
import static com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability.PREVIOUS_OFFSET;
import static com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability.VALUE_OFFSET;

import java.nio.ByteBuffer;

import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.WriteBackPortability;
import org.terracotta.offheapstore.storage.portability.WriteContext;

public class AttachedLinkedNode<T> implements LinkedNode<T> {

  private final Portability<? super T> valuePortability;
  private final ByteBuffer data;
  private final WriteContext writer;
  
  public AttachedLinkedNode(ByteBuffer buffer, Portability<? super T> valuePortability, WriteContext writer) {
    this.valuePortability = valuePortability;
    this.data = buffer;
    this.writer = writer;
  }
  
  @Override
  public long getLsn() {
    return getLong(LSN_OFFSET);
  }

  @Override
  public void setLsn(long lsn) {
    writer.setLong(LSN_OFFSET, lsn);
  }

  @Override
  public long getNext() {
    return getLong(NEXT_OFFSET);
  }

  @Override
  public long getPrevious() {
    return getLong(PREVIOUS_OFFSET);
  }

  @Override
  public void setNext(long encoding) {
    writer.setLong(NEXT_OFFSET, encoding);
  }

  @Override
  public void setPrevious(long encoding) {
    writer.setLong(PREVIOUS_OFFSET, encoding);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T getValue() {
    if (valuePortability instanceof WriteBackPortability<?>) {
      return (T) ((WriteBackPortability<? super T>) valuePortability).decode(getBuffer(VALUE_OFFSET), getWriteContext());
    } else {
      return (T) valuePortability.decode(getBuffer(VALUE_OFFSET));
    }
  }
  
  @Override
  public void flush() {
    writer.flush();
  }
  
  private ByteBuffer getBuffer(int offset) {
    if (offset < data.capacity()) {
      data.position(offset);
      return data.slice();
    } else {
      throw new IllegalArgumentException();
    }
  }
  
  private long getLong(final int address) {
    if (address + 8 < data.capacity()) {
      return data.getLong(address);
    } else {
      throw new IllegalArgumentException();
    }
  }
  
  @Override
  public String toString() {
    return getPrevious() + "<== OffHeapLinkedNode [lsn = " + getLsn() + "] ==> " + getNext();
  }

  @Override
  public int getMetadata() {
    throw new UnsupportedOperationException();
  }

  /**
   * A write context that passes the write back requests to the next
   * storage engine in the chain.
   *
   * @return a write context that can be used to write back individual longs offsetting into
   *         the generic type represented within {@code this} object.
   */
  private WriteContext getWriteContext() {
    return new WriteContext() {

      @Override
      public void setLong(int offset, long value) {
        writer.setLong(VALUE_OFFSET + offset, value);
      }

      @Override
      public void flush() {
        //no-op
      }
    };
  }
}