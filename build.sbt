name := "KafkaLiveSession"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies +=  "net.sf.jopt-simple" % "jopt-simple" % "5.0.2"
libraryDependencies += "com.iheart" %% "ficus" % "1.4.0"