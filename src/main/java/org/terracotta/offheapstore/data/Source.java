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
package org.terracotta.offheapstore.data;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;

/**
 *
 * @author cdennis
 */
public interface Source<T> {

  public T allocate(long l, boolean tableAllocationsSteal, boolean b, OffHeapStorageArea owner);

  public void free(T table);
  
}
