package sinktest

import java.util.Properties

import apitest.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //source
    val inputStream: DataStream[String] = env.addSource( new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties) )

    //transform
    val dataStream: DataStream[String] = inputStream
      .map( data =>{
        val dataArray = data.split(",")
        SensorReading( dataArray(0), dataArray(1).toLong, dataArray(2).toDouble ).toString
      })

    // 直接写入文件
//        dataStream.writeAsText("F:\\learning\\code\\flink_learning\\src\\main\\resources\\out")

//        dataStream.addSink( StreamingFileSink.forRowFormat[String](
//          new Path("F:\\learning\\code\\flink_learning\\src\\main\\resources\\out"),
//          new SimpleStringEncoder[String]("UTF-8")
//        ).build() )
    //sink
    dataStream.addSink( new FlinkKafkaProducer[String]("localhost:9092", "sinktest", new SimpleStringSchema()) )


    env.execute("kafka sink test job")
  }
}
