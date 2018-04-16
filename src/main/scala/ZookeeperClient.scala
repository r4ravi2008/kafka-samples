package com.kafkalivesession

import java.util.Properties

import com.typesafe.config.Config
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.log4j.LogManager

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Provides common zookeeper client functionality for Kafka
  */
class ZookeeperClient(config: Config) {

  private lazy val logger = LogManager.getRootLogger

  private[this] val zkUrl: String = config.as[String]("zookeeper.url")
  private[this] val sessionTimeout: Int = config.as[Int]("zookeeper.session.timeout")
  private[this] val connectionTimeout: Int = config.as[Int]("zookeeper.connection.timeout")
  private[this] val isZkSecurityEnabled: Boolean = config.as[Boolean]("zookeeper.security.enabled")
  private[this] val zkNodePath: String = config.as[String]("zookeeper.extractor.znode.path")
  private[this] val bootstrapServers: String = config.as[Set[String]]("kafka.brokers.list")
      .mkString(",")
  private[this] val keyDeserializer: String = config.as[String]("kafka.key.deserializer")
  private[this] val valueDeserializer: String = config.as[String]("kafka.value.deserializer")
  private[this] var zkUtils: Option[ZkUtils] = None

  /**
    * Returns [[Properties]] after reading configuration,
    * to be used for initializing connection to Zookeeper.
    *
    * @return [[Properties]] containing configuration.
    */
  private[this] def getProperties: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("group.id", "ZookeeperClient")
    props.put("key.deserializer", keyDeserializer)
    props.put("value.deserializer", valueDeserializer)
    props
  }

  /**
    * Gets number of partitions for given topic
    *
    * @param topicName Topic name
    * @return Number of partitions for the topic
    */
  def getPartitionCount(topicName: String): Int = {
    zkUtils = Option(ZkUtils(zkUrl, sessionTimeout, connectionTimeout, isZkSecurityEnabled))
    if (zkUtils.isDefined) {
      try {
        zkUtils.get.getPartitionsForTopics(Seq(topicName)).get(topicName).fold(0)(_.size)
      } finally {
        zkUtils.get.close()
      }
    } else 0
  }

  /**
    * Gets latest offsets for a given topic
    *
    * @param topicName Topic name
    * @return Map of partitionID and corresponding offset
    */
  def getLatestOffset(topicName: String): Map[Int, Long] = {
    val consumer = new KafkaConsumer[String, Array[Byte]](getProperties)
    val partitionsCount = getPartitionCount(topicName)
    val out = mutable.Map[Int, Long]()

    0 until partitionsCount map { partitionId =>
      val topicPartition = new TopicPartition(topicName, partitionId)
      consumer.assign(List(topicPartition).asJava)
      consumer.seekToEnd(List(topicPartition).asJava)
      out += partitionId -> consumer.position(topicPartition)
    }
    out.toMap
  }

  /**
    * Saves last offset information in Zookeeper
    *
    * @param sourceType Source type e.g. `netflow` or `pcap`
    * @param topicName Topic name
    * @param latestOffsets Map of partition ID and corresponding offset
    */
  def saveLatestOffset(sourceType: String, topicName: String,
      latestOffsets: Map[Int, Long]): Unit = {

    val zkPath: String = s"$zkNodePath/sources/$sourceType/topics/$topicName"
    try {
      zkUtils = Option(ZkUtils(zkUrl, sessionTimeout, connectionTimeout, isZkSecurityEnabled))
      if (zkUtils.isDefined) {
        for ((partId, offset) <- latestOffsets) {
          zkUtils.get.makeSurePersistentPathExists(s"$zkPath/partitions/$partId")
          zkUtils.get.updatePersistentPath(s"$zkPath/partitions/$partId", offset.toString)
        }
        logger.info(s"Saved latest offset information for topic '$topicName'")
      }
    }
    catch {
      case e: Exception => logger.error(s"Error while saving last offset in " +
          s"Zookeeper of topic: $topicName.", e)
    }
    finally {
      zkUtils.get.close()
    }
  }

  /**
    * Gets last saved offset in Zookeeper for a given topic
    *
    * @param sourceType Source type e.g. netflow or pcap
    * @param topicName Topic name
    * @throws InvalidTopicException If `topicName` doesn't exist
    * @return Map of partition IDs and corresponding offsets
    */
  @throws(classOf[InvalidTopicException])
  def getLastOffset(sourceType: String, topicName: String): Map[Int, Long] = {

    val zkPath: String = s"$zkNodePath/sources/$sourceType/topics/$topicName"
    val lastOffsets = mutable.Map[Int, Long]()

    try {
      zkUtils = Option(ZkUtils(zkUrl, sessionTimeout, connectionTimeout, isZkSecurityEnabled))
      if (zkUtils.isDefined) {
        val partitions = zkUtils.get.getPartitionsForTopics(Seq(topicName)).get(topicName)
        logger.info(s"Partitions: $partitions")
        if (partitions.isDefined) {
          partitions.get.foreach {
            partId =>
              val zkData = zkUtils.get.readDataMaybeNull(s"$zkPath/partitions/$partId")
              if (zkData._1.isDefined) {
                lastOffsets += partId -> zkData._1.get.toLong
              }
              else {
                lastOffsets += partId -> 0L
              }
          }
        }
        else throw new InvalidTopicException(s"Topic '$topicName' does not exist.")
      }
    }
    catch {
      case e: Exception => logger.error(s"Error while getting last offset for topic: $topicName", e)
    }
    finally {
      zkUtils.get.close()
    }
    lastOffsets.toMap
  }

  /**
    * Resets offsets for a given topic
    *
    * @param sourceType Source type e.g. `netflow` or `pcap`
    * @param topicName Topic name
    * @throws InvalidTopicException If `topicName` does not exist.
    */
  @throws(classOf[InvalidTopicException])
  def resetOffset(sourceType: String, topicName: String): Unit = {
    val zkPath = s"$zkNodePath/sources/$sourceType/topics/$topicName"

    try {
      zkUtils = Option(ZkUtils(zkUrl, sessionTimeout, connectionTimeout, isZkSecurityEnabled))
      if (zkUtils.isDefined) {
        val partitions = zkUtils.get.getPartitionsForTopics(Seq(topicName)).get(topicName)
        if (partitions.isDefined) {
          partitions.get.foreach(
            id => {
              zkUtils.get.makeSurePersistentPathExists(s"$zkPath/partitions/$id")
              zkUtils.get.updatePersistentPath(s"$zkPath/partitions/$id", "0")
            }
          )
          logger.info(s"Reset offsets for all partitions of topic '$topicName' to 0")
        }
        else throw new InvalidTopicException(s"Topic '$topicName' does not exist.")
      }
    }
    catch {
      case e: Exception => logger.error(s"Error while resetting offset of topic : $topicName", e)
    }
    finally {
      zkUtils.get.close()
    }
  }
}
