package com.kafkalivesession

import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object ScalaProducerExample extends App {
  val topic = args(1)
  val brokers = args(2)
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  for (nEvents <- Range(0, 1000)) {

    val data = new ProducerRecord[String, String](topic, nEvents.toString, nEvents.toString)
    producer.send(data)
  }
  System.out.println("sent per second: " + 1000 * 1000 / (System.currentTimeMillis()))
  producer.close()
}
