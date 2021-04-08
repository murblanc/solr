/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Test;

public class ZkDistributedLockTest extends SolrTestCaseJ4 {

  private static final String COLLECTION_NAME = "lockColl";
  final String SHARD_NAME = "lockShard";
  final String REPLICA_NAME = "lockReplica";

  static final int TIMEOUT = 10000;

  /**
   * Tests the obtention of a single read or write lock at a specific hierarchical level.
   * Tests the logic with a single thread, then tests multithreaded wait for lock acquire works.
   * Tests grouped to pay setup only once.
   */
  @Test
  public void testSingleLocks() throws Exception {
    Path zkDir = createTempDir("zkData");

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();
      try (SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT)) {
        DistributedCollectionLockFactory factory = new ZkDistributedCollectionLockFactory(zkClient, "/lockTestRoot");

        monothreadedTests(factory);
        multithreadedTests(factory);
      }
    } finally {
      server.shutdown();
    }
  }

  private void monothreadedTests(DistributedCollectionLockFactory factory) {
    // Collection level locks
    DistributedLock collRL1 = factory.createLock(false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
    assertTrue("collRL1 should have been acquired", collRL1.isAcquired());

    DistributedLock collRL2 = factory.createLock(false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
    assertTrue("collRL1 should have been acquired", collRL2.isAcquired());

    DistributedLock collWL3 = factory.createLock(true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
    assertFalse("collWL3 should not have been acquired, due to collRL1 and collRL2", collWL3.isAcquired());

    assertTrue("collRL2 should have been acquired, that should not have changed", collRL2.isAcquired());

    collRL1.release();
    collRL2.release();
    assertTrue("collWL3 should have been acquired, collRL1 and collRL2 were released", collWL3.isAcquired());

    DistributedLock collRL4 = factory.createLock(false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
    assertFalse("collRL4 should not have been acquired, due to collWL3 locking the collection", collRL4.isAcquired());

    // Collection is write locked by collWL3 and collRL4 read lock waiting behind. Now moving to request shard level locks.
    // These are totally independent from the Collection level locks so should see no impact.
    DistributedLock shardWL5 = factory.createLock(true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null);
    assertTrue("shardWL5 should have been acquired, there is no lock on that shard", shardWL5.isAcquired());

    DistributedLock shardWL6 = factory.createLock(true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null);
    assertFalse("shardWL6 should not have been acquired, shardWL5 is locking that shard", shardWL6.isAcquired());

    // Get a lock on a Replica. Again this is independent of collection or shard level
    DistributedLock replicaRL7 = factory.createLock(false, CollectionParams.LockLevel.REPLICA, COLLECTION_NAME, SHARD_NAME, REPLICA_NAME);
    assertTrue("replicaRL7 should have been acquired", replicaRL7.isAcquired());

    DistributedLock replicaWL8 = factory.createLock(true, CollectionParams.LockLevel.REPLICA, COLLECTION_NAME, SHARD_NAME, REPLICA_NAME);
    assertFalse("replicaWL8 should not have been acquired, replicaRL7 is read locking that replica", replicaWL8.isAcquired());

    replicaRL7.release();
    assertTrue("replicaWL8 should have been acquired, as replicaRL7 got released", replicaWL8.isAcquired());


    collWL3.release();
    assertTrue("collRL4 should have been acquired given collWL3 released", collRL4.isAcquired());
    shardWL5.release();
    assertTrue("shardWL6 should have been acquired, now that shardWL5 was released", shardWL6.isAcquired());

    replicaWL8.release();
    try {
      replicaWL8.isAcquired();
      fail("isAcquired() called after release() on a lock should have thrown exception");
    } catch (IllegalStateException ise) {
      // expected
    }

    // Releasing the collection lock used in the multithreaded phase
    collRL4.release();
  }

  private void multithreadedTests(DistributedCollectionLockFactory factory) throws Exception {
    // Acquiring right away a read lock
    DistributedLock readLock = factory.createLock(false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
    assertTrue("readLock should have been acquired", readLock.isAcquired());

    // And now creating a write lock, that can't be acquired just yet, because of the read lock
    DistributedLock writeLock = factory.createLock(true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
    assertFalse("writeLock should not have been acquired", writeLock.isAcquired());

    // Wait for acquisition of the write lock on another thread (and be notified via a latch)
    final CountDownLatch latch = new CountDownLatch(1);
    new Thread(() -> {
      writeLock.waitUntilAcquired();
      // countDown() will not be called if waitUntilAcquired() threw exception of any kind
      latch.countDown();
    }).start();

    // Wait for the thread to start and to get blocked in waitUntilAcquired()
    // (thread start could have been checked more reliably using another latch, and verifying the thread is in waitUntilAcquired
    // done through that thread stacktrace, but that would be overkill compared to the very slight race condition of waiting 30ms,
    // but a race that would not cause the test to fail since we're testing... that nothing happened yet).
    Thread.sleep(30);

    assertEquals("we should not have been notified that writeLock was acquired", 1, latch.getCount());
    assertFalse("writeLock should not have been acquired", writeLock.isAcquired());

    readLock.release();
    assertTrue("writeLock should have been acquired now that readlock was released", writeLock.isAcquired());

    // Wait for the Zookeeper watch to fire + the thread to be unblocked and countdown the latch
    // We'll wait up to 10 seconds here, so should be safe even if GC is extraordinarily high with a pause
    int i = 0;
    while (i < 1000 && latch.getCount() != 0) {
      Thread.sleep(10);
      i++;
    }
    assertEquals("we should have been notified that writeLock was acquired", 0, latch.getCount());
  }
}