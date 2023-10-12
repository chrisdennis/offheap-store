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
package org.terracotta.offheapstore;

/**
 *
 * @author Chris Dennis
 */
public interface MapInternals {

  /* Map oriented statistics */

  long getSize();

  long getTableCapacity();

  long getUsedSlotCount();

  long getRemovedSlotCount();

  int getReprobeLength();

  long getAllocatedMemory();

  long getOccupiedMemory();
  
  long getVitalMemory();

  long getDataAllocatedMemory();

  long getDataOccupiedMemory();
  
  long getDataVitalMemory();
  
  long getDataSize();
}
