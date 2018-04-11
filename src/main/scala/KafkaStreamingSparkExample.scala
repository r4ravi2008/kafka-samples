package com.kafkalivesession

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import joptsimple.{OptionParser, OptionSet, OptionSpec}
import net.ceedubs.ficus.Ficus._
import org.apache.log4j.LogManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.{SparkConf}

import scala.collection.JavaConverters._

/**
  * This spark application extracts records from specified Kafka topics and prints
  * consumed record values as String.
  */
object KafkaStreamingSparkExample {

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

    if (args.length == 0) {
      // scalastyle:off println
      System.err.println(s"Usage: ${this.getClass.getName}")
      // scalastyle:on println
      parser.printHelpOn(System.err)
      System.exit(1)
    }

    val params: OptionSet = parser.parse(args: _*)

    checkRequiredParams(parser, params, topicNameOpt)


    val config = ConfigFactory.load().getConfig("extractor")

    /*
     * Setup Spark job configuration
     */
    logger.info(s"Configuration: $config")
    val conf = new SparkConf().setAppName(config.as[String]("spark.app.name")).setMaster("local")
    conf.set("spark.streaming.kafka.consumer.poll.ms", "10000")
    val ssc = new StreamingContext(conf, Seconds(5))

    val preferredHosts = LocationStrategies.PreferConsistent

    val topic = params.valueOf(topicNameOpt).toLowerCase
    /*
     * Initialize Kafka RDD for fetching records from Kafka
     */
    val kafkaStream = KafkaUtils.createDirectStream(
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](
        util.Arrays.asList(topic),
        getKafkaParams(config)
      )
    )

    kafkaStream.foreachRDD(
      rdd => {
        logger.error(s"PRINTING BATCH")
        rdd.foreach{
          cr =>
            // scalastyle:off
            logger.error(cr.value())
        }
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
