package wc

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._

object FlinkKafkaConsumer extends App{
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "10.151.21.229:9092")
  properties.setProperty("group.id", "flink-group01")
  properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("auto.offset.reset", "latest")

  env.enableCheckpointing(5000) // 每隔 5000 毫秒 执行一次 checkpoint

//  val myKafkaConsumer = new FlinkKafkaConsumer[String]("topic-et", new SimpleStringSchema(), properties)
//  myKafkaConsumer.setStartFromLatest()

val myKafkaConsumer = new FlinkKafkaConsumer[String]("test01", new SimpleStringSchema(), properties)
//  myKafkaConsumer.setCommitOffsetsOnCheckpoints(true)

  //从开始消费
  myKafkaConsumer.setStartFromEarliest()
  val stream = env.addSource(myKafkaConsumer)
    .print()

  env.execute("Kafka Consumer App")
}
