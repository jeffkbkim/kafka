/**
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
package kafka.server

//import one.profiler.AsyncProfiler
import org.apache.kafka.common.metrics.{MetricConfig, Metrics}
import org.apache.kafka.common.test.api.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.test.api.ClusterTestExtensions
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.common.runtime.KafkaMetricHistogram
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
//import org.junit.jupiter.api.{AfterEach, BeforeEach}

import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT))
class OffsetCommitRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
//  private var profilerOutputPath: String = null
//  private var profiler: AsyncProfiler = null

//  @BeforeEach
//  def startProfiling(): Unit = {
//    profilerOutputPath = "/Users/jkim/dev/resources/investigation/offsetcommit/" + "0ms-wall-async-profiler-" + System.currentTimeMillis + ".jfr"
//    profiler = AsyncProfiler.getInstance
//    try {
//      profiler.execute(s"start,jfr,event=cpu,file=$profilerOutputPath")
//      println("Profiler started successfully.")
//    } catch {
//      case e: Exception =>
//        println(s"Failed to start profiler: ${e.getMessage}")
//        e.printStackTrace()
//    }
//  }
//
//  @AfterEach
//  def stopProfiling(): Unit = {
//    println("Stopping profiler...")
//    try {
//      profiler.stop()
//      println(s"Profiler stopped. Output saved to: $profilerOutputPath")
//    } catch {
//      case e: Exception =>
//        println(s"Failed to stop profiler: ${e.getMessage}")
//        e.printStackTrace()
//    }
//  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, value = "0"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testOffsetCommitDurationWithOldConsumerGroupProtocolAndNewGroupCoordinatorLinger0Ms(): Unit = {
    testOffsetCommitDuration(true)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, value = "5"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testOffsetCommitDurationWithOldConsumerGroupProtocolAndNewGroupCoordinatorLinger5Ms(): Unit = {
    testOffsetCommitDuration(true)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, value = "10"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testOffsetCommitDurationWithOldConsumerGroupProtocolAndNewGroupCoordinatorLinger10Ms(): Unit = {
    testOffsetCommitDuration(true)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, value = "20"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testOffsetCommitDurationWithOldConsumerGroupProtocolAndNewGroupCoordinatorLinger20Ms(): Unit = {
    testOffsetCommitDuration(true)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "false"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testOffsetCommitDurationWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testOffsetCommitDuration(false)
  }
  private def testOffsetCommitDuration(doPrintHistograms: Boolean): Unit = {
    val time = Time.SYSTEM

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    val metricConfig = new MetricConfig()
    val metrics = brokers().head.metrics

    val offsetCommitTimeHistogram = KafkaMetricHistogram.newLatencyHistogram(
      suffix =>
        kafkaMetricName(
          metrics,
          "offset-commit-total-time-ms-" + suffix
        )
    )

    // Join the consumer group. Note that we don't heartbeat here, so we must use
    // a session long enough for the duration of the test.
//    val (memberId, memberEpoch) = joinConsumerGroup("grp", useNewProtocol = false)

    // Define offset commit operation
    def commitOffsetWithMeasurement(iteration: Int): Unit = {
      println(s"------------------------Starting offset commit iteration $iteration------------------------")
      val startMs = time.milliseconds()
      commitOffset(
        groupId = "grp",
        memberId = "test-member-id",
        memberEpoch = -1,
        topic = "foo",
        partition = iteration % 3,
        offset = 100L + iteration,
        expectedError = Errors.NONE,
        version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
      )
      val endMs = time.milliseconds()
      offsetCommitTimeHistogram.record(metricConfig, endMs - startMs, endMs)
      println(s"------------------------Ending offset commit iteration. total time:${endMs - startMs}------------------------")
    }

    // Run offset commits synchronously
    (0 until 100).foreach { i =>
        commitOffsetWithMeasurement(i)
    }

    println("Final results:")
    offsetCommitTimeHistogram.stats().forEach { stat =>
      println(s"${stat.name().name()} - ${stat.stat().measure(metricConfig, time.milliseconds())}")
    }

    if (doPrintHistograms) printHistograms(metrics)
  }

  private def printHistograms(metrics: Metrics): Unit = {
    def printHistogram(metricNamePrefix: String): Unit = {
      var metricName = kafkaMetricName(metrics, metricNamePrefix + "-max")
      var metric = metrics.metrics.get(metricName)
      println(s"$metricNamePrefix-max: ${metric.metricValue}")

      metricName = kafkaMetricName(metrics, metricNamePrefix + "-p50")
      metric = metrics.metrics.get(metricName)
      println(s"$metricNamePrefix-p50: ${metric.metricValue}")

      metricName = kafkaMetricName(metrics, metricNamePrefix + "-p95")
      metric = metrics.metrics.get(metricName)
      println(s"$metricNamePrefix-p95: ${metric.metricValue}")

      metricName = kafkaMetricName(metrics, metricNamePrefix + "-p99")
      println(s"$metricNamePrefix-p99: ${metric.metricValue}")
    }

    val metricNames = Array("event-queue-time-ms", "event-processing-time-ms", "event-purgatory-time-ms", "batch-flush-time-ms", "batch-flush-interval-time-ms")
    metricNames.foreach(namePrefix => printHistogram(namePrefix))
  }

//  private def testOffsetCommitDurationConcurrent(): Unit = {
//    val time = Time.SYSTEM
//
//    // Creates the __consumer_offsets topics because it won't be created automatically
//    // in this test because it does not use FindCoordinator API.
//    createOffsetsTopic()
//
//    // Create the topic.
//    createTopic(
//      topic = "foo",
//      numPartitions = 3
//    )
//
//    val offsetCommitTimeHistogram = KafkaMetricHistogram.newLatencyHistogram(
//      suffix =>
//        kafkaMetricName(
//          new Metrics(),
//          "offsetCommitTotalTime-" + suffix,
//          "The " + suffix + " event queue time in milliseconds"
//        )
//    )
//    val metricConfig = new MetricConfig()
//
//    // Join the consumer group. Note that we don't heartbeat here, so we must use
//    // a session long enough for the duration of the test.
//    val (memberId, memberEpoch) = joinConsumerGroup("grp", useNewProtocol = false)
//
//    // Proper thread pool creation and management
//    val threadPool: ExecutorService = Executors.newFixedThreadPool(10)
//    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)
//
//    try {
//      // Define offset commit operation
//      def commitOffsetWithMeasurement(iteration: Int): Unit = {
//        val startMs = time.milliseconds()
//        commitOffset(
//          groupId = "grp",
//          memberId = memberId,
//          memberEpoch = memberEpoch,
//          topic = "foo",
//          partition = iteration % 3,
//          offset = 100L + iteration,
//          expectedError = Errors.NONE,
//          version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
//        )
//        val endMs = time.milliseconds()
//        offsetCommitTimeHistogram.record(metricConfig, endMs - startMs, endMs)
//      }
//
//      // Run offset commits concurrently
//      val futures = (0 until 100).map { i =>
//        Future {
//          commitOffsetWithMeasurement(i)
//        }
//      }
//
//      Await.result(Future.sequence(futures), 10.minutes)
//
//    } finally {
//      // Ensure thread pool is shut down
//      threadPool.shutdown()
//      if (!threadPool.awaitTermination(1, TimeUnit.MINUTES)) {
//        threadPool.shutdownNow()
//      }
//    }
//
//    println("Final results:")
//    offsetCommitTimeHistogram.stats().forEach { stat =>
//      println(s"${stat.name().name()} - ${stat.stat().measure(metricConfig, time.milliseconds())}")
//    }
//  }

  private def kafkaMetricName(metrics: Metrics, name: String) = metrics.metricName(name, "group-coordinator-metrics")
}
