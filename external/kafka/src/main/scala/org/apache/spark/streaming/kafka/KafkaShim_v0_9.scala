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

import kafka.utils.ZkUtils
import org.apache.kafka.common.security.JaasUtils
import org.apache.spark.util.Utils
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

class KafkaShim_v0_9 (val zkHost: String, var zkPort: Int) extends KafkaShim {
  private var zkUtils: ZkUtils = null
  // Flag to test whether the system is correctly started
  private var zkReady = false

  private var zookeeper: EmbeddedZookeeper = _

  // Zookeeper related configurations
  // private val zkHost = "localhost"
  // private var zkPort: Int = 0
  private val zkConnectionTimeout = 60000
  private val zkSessionTimeout = 6000


  def zkAddress: String = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper address")
    s"$zkHost:$zkPort"
  }

  def zookeeperUtils: ZkUtils = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper client")
    Option(zkUtils).getOrElse(
      throw new IllegalStateException("Zookeeper client is not yet initialized"))
  }

  // Set up the Embedded Zookeeper server and get the proper Zookeeper port
  private def setupEmbeddedZookeeper(): Unit = {
    // Zookeeper server startup
    zookeeper = new EmbeddedZookeeper(s"$zkHost:$zkPort")
    // Get the actual zookeeper binding port
    zkPort = zookeeper.actualPort
    val zkClient = ZkUtils.createZkClient(s"$zkHost:$zkPort", zkSessionTimeout, zkConnectionTimeout)
    zkUtils = ZkUtils(zkClient, JaasUtils.isZkSecurityEnabled())
    zkReady = true
  }


  override def createTopic(topic: String, partitions: Int, replicationFactor: Int): Unit = {

  }

  override def getLeaderForPartition(topic: String, partition: Int): Option[Int] = {
    None
  }

  override def updatePersistentPath(path: String, data: String): Unit = {

  }

  override def readDataMaybeNull(path: String): Option[(String, Stat)] = {
    None
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
