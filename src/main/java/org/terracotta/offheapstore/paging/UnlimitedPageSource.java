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

import java.nio.ByteBuffer;

import org.terracotta.offheapstore.buffersource.BufferSource;
import org.terracotta.offheapstore.data.ByteData;
import org.terracotta.offheapstore.data.Data;
import org.terracotta.offheapstore.data.Source;

/**
 *
 * @author cdennis
 */
public class UnlimitedPageSource implements Source<ByteData> {

  private final BufferSource source;

  public UnlimitedPageSource(BufferSource source) {
    this.source = source;
  }

  @Override
  public ByteData allocate(final long size, boolean thief, boolean victim, OffHeapStorageArea owner) {
    if (size > Integer.MAX_VALUE) {
      return null;
    } else {
      ByteBuffer buffer = source.allocateBuffer((int) size);
      if (buffer == null) {
        return null;
      } else {
        return Data.wrap(buffer);
      }
    }
  }

  @Override
  public void free(ByteData page) {
    //no-op
  }
}
