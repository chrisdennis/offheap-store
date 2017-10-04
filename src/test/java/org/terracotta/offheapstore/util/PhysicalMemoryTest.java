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
package org.terracotta.offheapstore.util;

import sun.security.util.SecurityConstants;
import java.lang.management.OperatingSystemMXBean;
import java.security.Permission;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;

/**
 *
 * @author cdennis
 */
public class PhysicalMemoryTest {

  @Test
  public void testMemoryInvariants() {
    Assert.assertThat(PhysicalMemory.freePhysicalMemory(), anyOf(nullValue(Long.class), lessThanOrEqualTo(PhysicalMemory.totalPhysicalMemory())));
    Assert.assertThat(PhysicalMemory.freeSwapSpace(), anyOf(nullValue(Long.class), lessThanOrEqualTo(PhysicalMemory.totalSwapSpace())));
    Assert.assertThat(PhysicalMemory.ourCommittedVirtualMemory(), anyOf(nullValue(Long.class), greaterThanOrEqualTo(0L)));
  }

  @Test
  public void testBehaviorWithSecurityManager() {
    Thread testThread = Thread.currentThread();
    System.setSecurityManager(new SecurityManager() {
      @Override
      public void checkPackageAccess(String pkg) {
        if (Thread.currentThread() == testThread && pkg.startsWith("com.sun.")) {
          throw new SecurityException();
        }
      }

      @Override
      public void checkPermission(Permission perm) {
        // To be allowed to set the security manager back to null at the end of the test
        if(perm.getName().equals("setSecurityManager")) {
          return;
        }

        // We care only about the test threads, other can do whatever they want
        if (Thread.currentThread() == testThread && perm.getName().equals(SecurityConstants.CHECK_MEMBER_ACCESS_PERMISSION)) {
          // OperatingSystemMXBean should not ask for permissions
          Class<?> stack[] = getClassContext();
          Arrays.stream(stack)
            .filter(c -> OperatingSystemMXBean.class.isAssignableFrom(c))
            .findAny()
            .ifPresent(c -> {
              System.out.println("Found " + c); throw new SecurityException(); });
        }

        super.checkPermission(perm);
      }
    });
    try {
      Assert.assertThat(PhysicalMemory.totalPhysicalMemory(), nullValue());
      Assert.assertThat(PhysicalMemory.freePhysicalMemory(), nullValue());
      Assert.assertThat(PhysicalMemory.totalSwapSpace(), nullValue());
      Assert.assertThat(PhysicalMemory.freeSwapSpace(), nullValue());
      Assert.assertThat(PhysicalMemory.ourCommittedVirtualMemory(), nullValue());
    } finally {
      System.setSecurityManager(null);
    }
  }
}
