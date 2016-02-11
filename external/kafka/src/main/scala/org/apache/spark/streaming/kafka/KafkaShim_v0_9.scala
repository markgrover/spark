/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.kafka

import java.net.InetSocketAddress

import kafka.admin.AdminUtils
import org.apache.spark.util.Utils
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

class KafkaShim_v0_9 (val zkHost: String, var zkPort: Int) extends KafkaShim {
  private var zkUtils: kafka.utils.ZkUtils = null

  // Zookeeper related configurations
  // private val zkHost = "localhost"
  // private var zkPort: Int = 0
  private val zkConnectionTimeout = 60000
  private val zkSessionTimeout = 6000

  // Set up the Embedded Zookeeper server and get the proper Zookeeper port
  def setup(): Unit = {
    val zkClient = kafka.utils.ZkUtils.createZkClient(s"$zkHost:$zkPort", zkSessionTimeout,
      zkConnectionTimeout)

    zkUtils = kafka.utils.ZkUtils(zkClient, org.apache.kafka.common.security.JaasUtils
      .isZkSecurityEnabled())
  }


  override def createTopic(topic: String, partitions: Int, replicationFactor: Int): Unit = {
    AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor)
  }

  override def getLeaderForPartition(topic: String, partition: Int): Option[Int] = {
    zkUtils.getLeaderForPartition(topic, partition)
  }

  override def updatePersistentPath(path: String, data: String): Unit = {
    zkUtils.updatePersistentPath(path, data)
  }

  override def readDataMaybeNull(path: String): (Option[String], Stat) = {
    zkUtils.readDataMaybeNull(path)
  }

  private class EmbeddedZookeeper(val zkConnect: String) {
    val snapshotDir = Utils.createTempDir()
    val logDir = Utils.createTempDir()

    val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
    val (ip, port) = {
      val splits = zkConnect.split(":")
      (splits(0), splits(1).toInt)
    }
    val factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(ip, port), 16)
    factory.startup(zookeeper)

    val actualPort = factory.getLocalPort

    def shutdown() {
      factory.shutdown()
      Utils.deleteRecursively(snapshotDir)
      Utils.deleteRecursively(logDir)
    }
  }


}
