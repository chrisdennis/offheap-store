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

module org.terracotta.offheap {

  requires slf4j.api;
  requires java.management;

  exports org.terracotta.offheapstore;
  exports org.terracotta.offheapstore.buffersource;
  exports org.terracotta.offheapstore.concurrent;
  exports org.terracotta.offheapstore.disk.paging;
  exports org.terracotta.offheapstore.disk.persistent;
  exports org.terracotta.offheapstore.disk.storage;
  exports org.terracotta.offheapstore.disk.storage.portability;
  exports org.terracotta.offheapstore.exceptions;
  exports org.terracotta.offheapstore.eviction;
  exports org.terracotta.offheapstore.jdk8;
  exports org.terracotta.offheapstore.paging;
  exports org.terracotta.offheapstore.pinning;
  exports org.terracotta.offheapstore.storage;
  exports org.terracotta.offheapstore.storage.listener;
  exports org.terracotta.offheapstore.storage.portability;
  exports org.terracotta.offheapstore.util;

}
