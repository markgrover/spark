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

import scala.util.Random

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfterAll

import org.apache.spark._

class KafkaRDDSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var kafkaTestUtils: KafkaTestUtils = _

  private val sparkConf = new SparkConf().setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)
  private var sc: SparkContext = _

  override def beforeAll {
    sc = new SparkContext(sparkConf)
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll {
    if (sc != null) {
      sc.stop
      sc = null
    }

    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  test("basic usage") {
    val topic = s"topicbasic-${Random.nextInt}"
    val (messages: Array[String], offsetRanges: Array[OffsetRange], kafkaParams: Map[String,
      String]) = setupBasicTest(topic)

    val rdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, offsetRanges)

    val received = rdd.map(_._2).collect.toSet
    assert(received === messages.toSet)

    // size-related method optimizations return sane results
    assert(rdd.count === messages.size)
    assert(rdd.countApprox(0).getFinalValue.mean === messages.size)
    assert(!rdd.isEmpty)
    assert(rdd.take(1).size === 1)
    assert(rdd.take(1).head._2 === messages.head)
    assert(rdd.take(messages.size + 10).size === messages.size)
  }

  test("test empty") {
    val topic = s"topicbasic-${Random.nextInt}"
    val (messages: Array[String], offsetRanges: Array[OffsetRange], kafkaParams: Map[String,
      String]) = setupBasicTest(topic)
    val emptyRdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, Array(OffsetRange(topic, 0, 0, 0)))

    assert(emptyRdd.isEmpty)
  }


  test("invalid offset ranges") {
    val topic = s"topicbasic-${Random.nextInt}"
    val (messages: Array[String], offsetRanges: Array[OffsetRange], kafkaParams: Map[String,
      String]) = setupBasicTest(topic)
    // invalid offset ranges throw exceptions
    val badRanges = Array(OffsetRange(topic, 0, 0, messages.size + 1))
    intercept[SparkException] {
      KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
        sc, kafkaParams, badRanges)
    }
  }

  private def setupBasicTest(topic: String): (Array[String], Array[OffsetRange], Map[String,
    String]) = {
    kafkaTestUtils.createTopic(topic)
    val messages = Array("the", "quick", "brown", "fox")
    kafkaTestUtils.sendMessages(topic, messages)

    val offsetRanges = Array(OffsetRange(topic, 0, 0, messages.size))

    val kafkaParams = Map("metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "group.id" -> s"test-consumer-${Random.nextInt}")
    (messages, offsetRanges, kafkaParams)
  }

  private def setupNewBasicTest(topic: String): (Array[String], Array[OffsetRange], Map[String,
    String]) = {
    kafkaTestUtils.createTopic(topic)
    val messages = Array("the", "quick", "brown", "fox")
    kafkaTestUtils.sendMessages(topic, messages)

    val offsetRanges = Array(OffsetRange(topic, 0, 0, messages.size))

    val kafkaParams = Map("bootstrap.servers" -> kafkaTestUtils.brokerAddress,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> s"test-consumer-${Random.nextInt}")
    (messages, offsetRanges, kafkaParams)
  }

  // TODO: Renable when we move to Kafka 0.9.0.1. This test wouldn't pass until KAFKA-3029
  // (Make class org.apache.kafka.common.TopicPartition Serializable) is resolved.
  ignore("basic usage with new Kafka consumer API") {
    val topic = s"topicbasic-${Random.nextInt}"
    val (messages: Array[String], offsetRanges: Array[OffsetRange], kafkaParams: Map[String,
      String]) = setupNewBasicTest(topic)

    val rdd = KafkaUtils.createNewRDD[String, String](sc, kafkaParams, offsetRanges)

    val received = rdd.map(_._2).collect.toSet
    assert(received === messages.toSet)

    // size-related method optimizations return sane results
    assert(rdd.count === messages.size)
    assert(rdd.countApprox(0).getFinalValue.mean === messages.size)
    assert(!rdd.isEmpty)
    assert(rdd.take(1).size === 1)
    assert(rdd.take(1).head._2 === messages.head)
    assert(rdd.take(messages.size + 10).size === messages.size)
  }

  test("test empty with new Kafka consumer API") {
    val topic = s"topicbasic-${Random.nextInt}"
    val (messages: Array[String], offsetRanges: Array[OffsetRange], kafkaParams: Map[String,
      String]) = setupNewBasicTest(topic)
    val emptyRdd = KafkaUtils.createNewRDD[String, String](
      sc, kafkaParams, Array(OffsetRange(topic, 0, 0, 0)))

    assert(emptyRdd.isEmpty)
  }

  test("invalid offset ranges with new Kafka consumer API") {
    val topic = s"topicbasic-${Random.nextInt}"
    val (messages: Array[String], offsetRanges: Array[OffsetRange], kafkaParams: Map[String,
      String]) = setupNewBasicTest(topic)
    // invalid offset ranges throw exceptions
    val badRanges = Array(OffsetRange(topic, 0, 0, messages.size + 1))
    intercept[SparkException] {
      KafkaUtils.createNewRDD[String, String](sc, kafkaParams, badRanges)
    }
  }

  test("iterator boundary conditions") {
    // the idea is to find e.g. off-by-one errors between what kafka has available and the rdd
    val topic = s"topicboundary-${Random.nextInt}"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    kafkaTestUtils.createTopic(topic)

    val kafkaParams = Map("metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "group.id" -> s"test-consumer-${Random.nextInt}")

    val kc = new KafkaCluster(kafkaParams)

    // this is the "lots of messages" case
    kafkaTestUtils.sendMessages(topic, sent)
    val sentCount = sent.values.sum

    // rdd defined from leaders after sending messages, should get the number sent
    val rdd = getRdd(kc, Set(topic))

    assert(rdd.isDefined)

    val ranges = rdd.get.asInstanceOf[HasOffsetRanges].offsetRanges
    val rangeCount = ranges.map(o => o.untilOffset - o.fromOffset).sum

    assert(rangeCount === sentCount, "offset range didn't include all sent messages")
    assert(rdd.get.count === sentCount, "didn't get all sent messages")

    val rangesMap = ranges.map(o => TopicAndPartition(o.topic, o.partition) -> o.untilOffset).toMap

    // make sure consumer offsets are committed before the next getRdd call
    kc.setConsumerOffsets(kafkaParams("group.id"), rangesMap).fold(
      err => throw new Exception(err.mkString("\n")),
      _ => ()
    )

    // this is the "0 messages" case
    val rdd2 = getRdd(kc, Set(topic))
    // shouldn't get anything, since message is sent after rdd was defined
    val sentOnlyOne = Map("d" -> 1)

    kafkaTestUtils.sendMessages(topic, sentOnlyOne)

    assert(rdd2.isDefined)
    assert(rdd2.get.count === 0, "got messages when there shouldn't be any")

    // this is the "exactly 1 message" case, namely the single message from sentOnlyOne above
    val rdd3 = getRdd(kc, Set(topic))
    // send lots of messages after rdd was defined, they shouldn't show up
    kafkaTestUtils.sendMessages(topic, Map("extra" -> 22))

    assert(rdd3.isDefined)
    assert(rdd3.get.count === sentOnlyOne.values.sum, "didn't get exactly one message")

  }

  test("iterator boundary conditions using new Kafka consumer API") {
    // the idea is to find e.g. off-by-one errors between what kafka has available and the rdd
    val topic = s"topicboundary-${Random.nextInt}"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    kafkaTestUtils.createTopic(topic)

    val kafkaParams = Map("bootstrap.servers" -> kafkaTestUtils.brokerAddress,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> s"test-consumer-${Random.nextInt}")

    val kc = new NewKafkaCluster(kafkaParams)

    // this is the "lots of messages" case
    kafkaTestUtils.sendMessages(topic, sent)
    val sentCount = sent.values.sum

    // rdd defined from leaders after sending messages, should get the number sent
    val rdd = getNewRdd(kc, Set(topic))

    assert(rdd.isDefined)

    val ranges = rdd.get.asInstanceOf[HasOffsetRanges].offsetRanges
    val rangeCount = ranges.map(o => o.untilOffset - o.fromOffset).sum

    assert(rangeCount === sentCount, "offset range didn't include all sent messages")
    assert(rdd.get.count === sentCount, "didn't get all sent messages")

    val rangesMap = ranges.map(o => new TopicPartition(o.topic, o.partition) -> o.untilOffset).toMap

    // make sure consumer offsets are committed before the next getRdd call
    kc.setConsumerOffsets(rangesMap)

    // this is the "0 messages" case
    val rdd2 = getNewRdd(kc, Set(topic))
    // shouldn't get anything, since message is sent after rdd was defined
    val sentOnlyOne = Map("d" -> 1)

    kafkaTestUtils.sendMessages(topic, sentOnlyOne)

    assert(rdd2.isDefined)
    assert(rdd2.get.count === 0, "got messages when there shouldn't be any")

    // this is the "exactly 1 message" case, namely the single message from sentOnlyOne above
    val rdd3 = getNewRdd(kc, Set(topic))
    // send lots of messages after rdd was defined, they shouldn't show up
    kafkaTestUtils.sendMessages(topic, Map("extra" -> 22))

    assert(rdd3.isDefined)
    assert(rdd3.get.count === sentOnlyOne.values.sum, "didn't get exactly one message")

  }

  // get an rdd from the committed consumer offsets until the latest leader offsets,
  private def getRdd(kc: KafkaCluster, topics: Set[String]) = {
    val groupId = kc.kafkaParams("group.id")
    def consumerOffsets(topicPartitions: Set[TopicAndPartition]) = {
      kc.getConsumerOffsets(groupId, topicPartitions).right.toOption.orElse(
        kc.getEarliestLeaderOffsets(topicPartitions).right.toOption.map { offs =>
          offs.map(kv => kv._1 -> kv._2.offset)
        }
      )
    }
    kc.getPartitions(topics).right.toOption.flatMap { topicPartitions =>
      consumerOffsets(topicPartitions).flatMap { from =>
        kc.getLatestLeaderOffsets(topicPartitions).right.toOption.map { until =>
          val offsetRanges = from.map { case (tp: TopicAndPartition, fromOffset: Long) =>
              OffsetRange(tp.topic, tp.partition, fromOffset, until(tp).offset)
          }.toArray

          val leaders = until.map { case (tp: TopicAndPartition, lo: LeaderOffset) =>
              tp -> Broker(lo.host, lo.port)
          }.toMap

          KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder, String](
            sc, kc.kafkaParams, offsetRanges, leaders,
            (mmd: MessageAndMetadata[String, String]) => s"${mmd.offset} ${mmd.message}")
        }
      }
    }
  }

  // get an rdd from the committed consumer offsets until the latest leader offsets,
  private def getNewRdd(kc: NewKafkaCluster[_, _], topics: Set[String]) = {
    val groupId = kc.kafkaParams("group.id")
    val topicPartitions = kc.getPartitions(topics)
    val consumerOffsets = try {
      kc.getCommittedOffsets(topicPartitions)
    } catch {
      case e: SparkException => kc.getEarliestOffsets(topicPartitions)
    }
    val latestOffsets = kc.getLatestOffsets(topicPartitions)

    val offsetRanges = consumerOffsets.map { case (tp: TopicPartition, fromOffset: Long) =>
      OffsetRange(tp.topic, tp.partition, fromOffset, latestOffsets(tp))
    }.toArray

    Option(KafkaUtils.createNewRDD[String, String, String](
      sc, kc.kafkaParams, offsetRanges,
      (cr: ConsumerRecord[String, String]) => s"${cr.offset()} ${cr.value()}"))
  }

}
