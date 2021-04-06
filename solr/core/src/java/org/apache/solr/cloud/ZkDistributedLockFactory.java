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

import com.google.common.base.Preconditions;
import org.apache.solr.cloud.api.collections.DistributedCollectionCommandRunner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionParams;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
 * A distributed lock implementation using Zookeeper "directory" nodes created within the collection znode hierarchy.
 * The locks are implemented using ephemeral nodes placed below the "directory" nodes.
 *
 * @see <a href="https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_Locks">Zookeeper lock recipe</a>
 */
public class ZkDistributedLockFactory implements DistributedLockFactory {

  private final SolrZkClient zkClient;
  private final String rootPath;

  public ZkDistributedLockFactory(SolrZkClient zkClient, String rootPath) {
    this.zkClient = zkClient;
    this.rootPath = rootPath;
  }


  public DistributedLock createLock(boolean isWriteLock, CollectionParams.LockLevel level, String collName, String shardId,
                                    String replicaName) {
    Preconditions.checkArgument(collName != null, "collName can't be null");
    Preconditions.checkArgument(level == CollectionParams.LockLevel.COLLECTION || shardId != null,
        "shardId can't be null when getting lock for shard or replica");
    Preconditions.checkArgument(level != CollectionParams.LockLevel.REPLICA || replicaName != null,
        "replicaName can't be null when getting lock for replica");

    String lockPath = getLockPath(level, collName, shardId, replicaName);

    try {
      // TODO optimize by first attempting to create the ZkDistributedLock without calling makeLockPath() and only call it
      //  if the lock creation fails. This will be less costly on high contention (and slightly more on low contention)
      makeLockPath(lockPath);

      return isWriteLock ? new ZkDistributedLock.Write(zkClient, lockPath) : new ZkDistributedLock.Read(zkClient, lockPath);
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Returns the Zookeeper path to the lock, creating missing nodes if needed.
   * Note that the complete lock hierarchy (/locks and below) can be deleted if SolrCloud is stopped.
   *
   * The tree of lock directories for a given collection {@code collName} is as follows:
   * <pre>
   *   rootPath/
   *      collName/
   *         Locks   <-- EPHEMERAL collection level locks go here
   *         _ShardName1/
   *            Locks   <-- EPHEMERAL shard level locks go here
   *            _replicaNameS1R1   <-- EPHEMERAL replica level locks go here
   *            _replicaNameS1R2   <-- EPHEMERAL replica level locks go here
   *         _ShardName2/
   *            Locks   <-- EPHEMERAL shard level locks go here
   *            _replicaNameS2R1   <-- EPHEMERAL replica level locks go here
   *            _replicaNameS2R2   <-- EPHEMERAL replica level locks go here
   * </pre>
   * This method will create the path where the {@code EPHEMERAL} lock nodes should go. That path is:
   * <ul>
   *   <li>For {@link org.apache.solr.common.params.CollectionParams.LockLevel#COLLECTION} -
   *   {@code /locks/collections/collName/Locks}</li>
   *   <li>For {@link org.apache.solr.common.params.CollectionParams.LockLevel#SHARD} -
   *   {@code /locks/collections/collName/_shardName/Locks}</li>
   *   <li>For {@link org.apache.solr.common.params.CollectionParams.LockLevel#REPLICA} -
   *   {@code /locks/collections/collName/_shardName/_replicaName}. There is no {@code Locks} subnode here because replicas do
   *   not have children so no need to separate {@code EPHEMERAL} lock nodes from children nodes as is the case for shards
   *   and collections</li>
   * </ul>
   * Note the {@code _} prefixing shards and replica names is to support shards or replicas called "{@code Locks}".
   * Also note the returned path does not contain the separator ({@code "/"}) at the end.
   */
  private String getLockPath(CollectionParams.LockLevel level, String collName, String shardId, String replicaName) {
    StringBuilder path = new StringBuilder(100);
    path.append(rootPath).append(DistributedCollectionCommandRunner.ZK_PATH_SEPARATOR).append(collName).append(DistributedCollectionCommandRunner.ZK_PATH_SEPARATOR);

    final String LOCK_NODENAME = "Locks"; // Should not start with SUBNODE_PREFIX :)
    final String SUBNODE_PREFIX = "_";

    if (level == CollectionParams.LockLevel.COLLECTION) {
      return path.append(LOCK_NODENAME).toString();
    } else if (level == CollectionParams.LockLevel.SHARD) {
      return path.append(SUBNODE_PREFIX).append(shardId).append(DistributedCollectionCommandRunner.ZK_PATH_SEPARATOR).append(LOCK_NODENAME).toString();
    } else if (level == CollectionParams.LockLevel.REPLICA) {
      return path.append(SUBNODE_PREFIX).append(shardId).append(DistributedCollectionCommandRunner.ZK_PATH_SEPARATOR).append(SUBNODE_PREFIX).append(replicaName).toString();
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported lock level " + level);
    }
  }



  private void makeLockPath(String lockNodePath) throws KeeperException, InterruptedException {
    try {
      if (!zkClient.exists(lockNodePath, true)) {
        zkClient.makePath(lockNodePath, new byte[0], CreateMode.PERSISTENT, true);
      }
    } catch (KeeperException.NodeExistsException nee) {
      // Some other thread (on this or another JVM) beat us to create the node, that's ok.
    }
  }
}
