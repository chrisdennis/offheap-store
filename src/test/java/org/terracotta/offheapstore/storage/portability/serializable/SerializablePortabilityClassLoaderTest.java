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
package org.terracotta.offheapstore.storage.portability.serializable;

import java.io.Serializable;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;
import org.terracotta.offheapstore.util.DuplicateClassLoader;

/**
 * @author teck
 */
public class SerializablePortabilityClassLoaderTest {

  @Test
  public void testThreadContextLoader() throws Exception {
    ClassLoader loader = new DuplicateClassLoader(SerializablePortabilityClassLoaderTest.class.getClassLoader());
    Portability<Serializable> portability = new SerializablePortability();

    ByteBuffer encoded = portability.encode((Serializable) loader.loadClass(Foo.class.getName()).newInstance());

    final ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(loader);
      Assert.assertSame(loader, portability.decode(encoded).getClass().getClassLoader());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  public void testExplicitLoader() throws Exception {
    ClassLoader loader = new DuplicateClassLoader(SerializablePortabilityClassLoaderTest.class.getClassLoader());
    Portability<Serializable> portability = new SerializablePortability(loader);

    ByteBuffer encoded = portability.encode((Serializable) loader.loadClass(Foo.class.getName()).newInstance());

    final ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      // setting TCCL doesn't matter here, but set it to make sure it doesn't get used
      Thread.currentThread().setContextClassLoader(new DuplicateClassLoader(SerializablePortabilityClassLoaderTest.class.getClassLoader()));
      Assert.assertSame(loader, portability.decode(encoded).getClass().getClassLoader());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @SuppressWarnings("serial")
  public static class Foo implements Serializable {
    //
  }

}
