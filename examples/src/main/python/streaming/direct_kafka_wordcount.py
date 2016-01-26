#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Counts words in UTF8 encoded, '\n' delimited text directly received from Kafka in every 2 seconds.
 Usage: direct_kafka_wordcount.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      examples/src/main/python/streaming/direct_kafka_wordcount.py \
      localhost:9092 test`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import printUsage

def printUsage():
    print("Usage: direct_kafka_wordcount.py <broker_list> <topic> [<'old'|'new'>]", file=sys.stderr)
    print("Broker list is a comma separated list with no spaces. The last optional", file=sys.stderr)
    print("argument specifies what kafka consumer API to use. Defaults to old.", file=sys.stderr)

if __name__ == "__main__":

    apiOldOrNew = "old"
    if len(sys.argv) == 3:
        brokers, topic = sys.argv[1:]
    elif len(sys.argv) == 4:
        brokers, topic, apiOldOrNew = sys.argv[1:]
    else:
        printUsage()
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)

    if (apiOldOrNew.lower() == "old"):
        kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers,
                                                           "auto.offset.reset": "smallest"})
    elif (apiOldOrNew.lower() == "new"):
        kvs = KafkaUtils.createNewDirectStream(ssc, [topic], {"bootstrap.servers": brokers,
                                                              "group.id": "x",
                                                              "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                                                              "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                                                              "auto.offset.reset": "earliest"})
    else:
        print("Api value was %s. Expected old or new" % apiOldOrNew)
        printUsage()
        exit(-1)

    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
