package apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author Wu Ming
 * @Date 2020/8/7 10:34
 * @Version 1.0
 */
object windowtest02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //使用EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //  //每隔0.1s产生一个watermark,如果不设置，那么在设置时间语义的时候会自动设置200ms
    env.getConfig.setAutoWatermarkInterval(100L)

    println(Long.MinValue)
    //    val stream: DataStream[String] = env.readTextFile("E:\\myproject\\flinkDemo\\src\\main\\resources\\sensor.txt")
    val stream: DataStream[String] = env.socketTextStream("172.31.40.135", 8888)
    val datastream: DataStream[SensorReading] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //      .assignAscendingTimestamps(_.timestamp)
      .assignTimestampsAndWatermarks(new MyAssigner())
    //      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
    //        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
    //      })

    //统计10秒内的最小温度
    val minTempPerWindowStream: DataStream[(String, Double)] = datastream
      .map(data => (data.id, data.temp))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce((reducedData, data) => (reducedData._1, reducedData._2.min(data._2))) //用reduce做增量聚合

    datastream.print("inputData")
    minTempPerWindowStream.print("minTempPerTenSec")

    env.execute("windowtest02")
  }
}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound: Long = 1 * 1000L
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    if (maxTs == Long.MinValue)
      new Watermark(maxTs)
    else
      new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}