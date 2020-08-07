package sinktest

import java.util

import apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.table.descriptors.Elasticsearch
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object esSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val inputStream: DataStream[String] = env.readTextFile("F:\\learning\\code\\flink_learning\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream
      .map( data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })

    val httpHosts: java.util.ArrayList[HttpHost] = new java.util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    val esSinkFunc: ElasticsearchSinkFunction[SensorReading] = new ElasticsearchSinkFunction[SensorReading]{
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //包装成一个map或者jsonobject
        val dataSource = new util.HashMap[String,String]()
        dataSource.put("sensor_id",t.id)
        dataSource.put("tmp",t.temp.toString)
        dataSource.put("ts",t.timestamp.toString)

        //创建index Request，准备发送数据
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("data")
          .source(dataSource)
        //利用RequestIndexer发送请求，写入数据
        requestIndexer.add(indexRequest)

        println("data " + t + " saved successfully")

      }

    }

    dataStream.addSink(new ElasticsearchSink
      .Builder[SensorReading](httpHosts,esSinkFunc)
      .build())

    env.execute("es test job")
  }
}
