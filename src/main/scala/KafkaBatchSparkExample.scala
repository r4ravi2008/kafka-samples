package com.kafkalivesession

import com.typesafe.config.{Config, ConfigFactory}
import joptsimple.{OptionParser, OptionSet, OptionSpec}
import net.ceedubs.ficus.Ficus._
import org.apache.log4j.LogManager
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * This spark application extracts records from specified Kafka topics and prints
  * consumed record values as String.
  */
object KafkaBatchSparkExample {

  private lazy val logger = LogManager.getRootLogger

  /**
    * Returns a [[java.util.Map]] of `Kafka` related property names and corresponding values.
    *
    * @param config [[com.typesafe.config.Config]] object representing Current configuration.
    * @return [[java.util.Map]] of Kafka property names and their values.
    */
  private def getKafkaParams(config: Config) = Map[String, Object](
    "bootstrap.servers" -> config.as[Set[String]]("kafka.brokers.list").mkString(","),
    "key.deserializer" -> config.as[String]("kafka.key.deserializer"),
    "value.deserializer" -> config.as[String]("kafka.value.deserializer"),
    "group.id" -> "CDAP-Extractor"
  ).asJava

  /**
    * Validates that all mandatory parameter are provided.
    *
    * @param parser         Option parser instance.
    * @param params         Provided parameters
    * @param requiredParams Mandatory parameters
    */
  def checkRequiredParams(parser: OptionParser,
      params: OptionSet,
      requiredParams: OptionSpec[_]*): Unit = {
    for (param <- requiredParams) {
      if (!params.has(param)) {
        // scalastyle:off println
        System.err.println(s"Missing required argument : $param")
        // scalastyle:on println
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser()
    val topicNameOpt = parser.accepts("topic", "Topic name to consume events from.")
        .withRequiredArg()
        .describedAs("topic-name")
        .ofType(classOf[String])
    val fromOffsetOpt = parser
        .accepts("from-offsets", "Start extracting from these offsets, for the configured topic.")
        .withRequiredArg()
        .describedAs("p0,p1,..,pn")
        .ofType(classOf[String])
    val toOffsetOpt = parser
        .accepts("to-offsets", "Extract till these offsets, for the configured topic.")
        .withRequiredArg()
        .describedAs("p0,p1,..,pn")
        .ofType(classOf[String])
    val noSaveOffsetOpt = parser.accepts("no-save-offset", "Do not save offsets when done.")

    if (args.length == 0) {
      // scalastyle:off println
      System.err.println(s"Usage: ${this.getClass.getName}")
      // scalastyle:on println
      parser.printHelpOn(System.err)
      System.exit(1)
    }

    val params: OptionSet = parser.parse(args: _*)

    checkRequiredParams(parser, params, topicNameOpt)

    val topic = params.valueOf(topicNameOpt).toLowerCase
    val fromOffsetValue: Option[String] =
      if (params.has(fromOffsetOpt)) Option(params.valueOf(fromOffsetOpt)) else Option("-1")
    val toOffsetValue: Option[String] =
      if (params.has(toOffsetOpt)) Option(params.valueOf(toOffsetOpt)) else Option("-1")
    val noSaveOffset: Boolean = params.has(noSaveOffsetOpt)

    val config = ConfigFactory.load().getConfig("extractor")

    // val topic = config.as[String]("kafka.topic.name")

    var fromOffsets: Option[Map[Int, Long]] = None
    var untilOffsets: Option[Map[Int, Long]] = None

    // scalastyle:off
    var zkClient: ZookeeperClient = null
    // scalastyle:on
    try {
      zkClient = new ZookeeperClient(config)
    } catch {
      case e: Exception => logger.error(s"Error initializing Zookeeper client.", e)
    }

    fromOffsetValue.get match {
      case "-1" => fromOffsets = Option(zkClient.getLastOffset("extractor_test", topic))
      case v: String =>
        val userOffsets = v.split(",").map(_.toLong)
        val numPartitions = zkClient.getPartitionCount(topic)
        if (userOffsets.length != numPartitions) {
          // scalastyle:off println
          System.out.println(s"--from-offset : ")
          System.err.println(s"Number of offsets provided is ${userOffsets.length} but " +
              s"number of partitions for topic '$topic' is $numPartitions.")
          // scalastyle:on println
          System.exit(2)
        } else {
          var offsets = collection.mutable.Map[Int, Long]()
          0 until numPartitions foreach { i =>
            offsets += i -> userOffsets(i)
          }
          fromOffsets = Option(offsets.toMap)
        }
    }

    toOffsetValue.get match {
      case "-1" => untilOffsets = Option(zkClient.getLatestOffset(topic))
      case v: String =>
        val userOffsets = v.split(",").map(_.toLong)
        val numPartitions = zkClient.getPartitionCount(topic)
        if (userOffsets.length != numPartitions) {
          // scalastyle:off println
          System.out.println(s"--to-offset : ")
          System.err.println(s"Number of offsets provided is ${userOffsets.length} but " +
              s"number of partitions for topic '$topic' is $numPartitions.")
          // scalastyle:on println
          System.exit(2)
        } else {
          var offsets = collection.mutable.Map[Int, Long]()
          0 until numPartitions foreach { i =>
            offsets += i -> userOffsets(i)
          }
          untilOffsets = Option(offsets.toMap)
        }
    }

    /*
     * Setup Spark job configuration
     */
    logger.info(s"Configuration: $config")
    val conf = new SparkConf().setAppName(config.as[String]("spark.app.name")).setMaster("local")
    conf.set("spark.streaming.kafka.consumer.poll.ms", "10000")
    val sc = new SparkContext(conf)


    val preferredHosts = LocationStrategies.PreferConsistent
    val offsetRanges: ArrayBuffer[OffsetRange] = ArrayBuffer()

    /*
     * Log information about kafka offsets
     */
    logger.info(s"Kafka topic name: $topic with partitions: ${zkClient.getPartitionCount(topic)}")
    if (untilOffsets.isDefined) {
      for ((partitionId, untilOffset) <- untilOffsets.get) {
        val fromOffset = fromOffsets.get(partitionId)
        logger.info(s"Processing offset range for partition id: $partitionId " +
            s"(from-offset: $fromOffset, to-offset: $untilOffset)")
        offsetRanges += OffsetRange(
          topic,
          partitionId,
          fromOffset,
          untilOffset)
      }
      logger.info(s"Total offset ranges to process: ${offsetRanges.length}")
    }

    /*
     * Initialize Kafka RDD for fetching records from Kafka
     */
    val kafkaRdd = KafkaUtils.createRDD[String, String](
      sc,
      getKafkaParams(config),
      offsetRanges.toArray,
      preferredHosts
    )

    /*
     * Convert Array[Byte] to Avro records and de-duplicate.
     */
    val strValueRecords = kafkaRdd.mapPartitions { partition =>
      partition.map(cr => cr.value())
    }

    // scalastyle:off
    strValueRecords.foreach(logger.error)
    // scalastyle:on
    // Save latest offsets in Zookeeper unless `noSaveOffset` is enabled.
    if (!noSaveOffset) {
      zkClient.saveLatestOffset("extractor_test", topic, untilOffsets.get)
    }
  }
}
