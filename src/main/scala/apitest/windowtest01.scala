package apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author Wu Ming
 * @Date 2020/8/7 9:57
 * @Version 1.0
 */
object windowtest01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //使用EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //  //每隔0.1s产生一个watermark
    //  env.getConfig.setAutoWatermarkInterval(100L)

    //    val stream: DataStream[String] = env.readTextFile("E:\\myproject\\flinkDemo\\src\\main\\resources\\sensor.txt")
    val stream: DataStream[String] = env.socketTextStream("172.31.40.135", 8888)
    val datastream: DataStream[SensorReading] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    //统计10秒内的最小温度
    val minTempPerWindowStream: DataStream[(String, Double)] = datastream
      .map(data => (data.id, data.temp))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce((reducedData, data) => (reducedData._1, reducedData._2.min(data._2))) //用reduce做增量聚合

    datastream.print("inputData")
    minTempPerWindowStream.print("minTempPerTenSec")

    env.execute("windowtest01")
  }
}
