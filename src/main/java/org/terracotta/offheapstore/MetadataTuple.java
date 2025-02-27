/*
 * Copyright 2016-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore;

public final class MetadataTuple<V> {

  private final V value;
  private final int metadata;

  private MetadataTuple(V value, int metadata) {
    this.value = value;
    this.metadata = metadata;
  }

  public V value() {
    return value;
  }

  public int metadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MetadataTuple<?>) {
      MetadataTuple<?> other = (MetadataTuple<?>) o;
      return value().equals(other.value()) && metadata() == other.metadata();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return value.hashCode() ^ metadata;
  }


  public static <T> MetadataTuple<T> metadataTuple(T value, int metadata) {
    return new MetadataTuple<>(value, metadata);
  }
}
