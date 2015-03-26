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
package org.terracotta.offheapstore.storage.listener;

import java.nio.ByteBuffer;


public interface RuntimeStorageEngineListener<K, V> extends StorageEngineListener<K, V> {

  void written(K key, V value, ByteBuffer binaryKey, ByteBuffer binaryValue, long hash, int metadata, long encoding);
  
  void freed(long encoding, long hash, ByteBuffer binaryKey, boolean removed);

  void cleared();

  void copied(long hash, long oldEncoding, long newEncoding, int metadata);
}
