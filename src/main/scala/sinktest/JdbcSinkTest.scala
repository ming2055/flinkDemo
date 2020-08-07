package sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度为1，在jdbc写入或者更新数据的时候才能成功，否则会因为主键重复的问题发生错误
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("E:\\myproject\\flinkDemo\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream
      .map( data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })

    dataStream.addSink(new MyJdbcSink())
    env.execute("jdbc sink test job")

  }
}

class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  //定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1,value.temp)
    updateStmt.setString(2,value.id)
    updateStmt.execute()
    println(updateStmt.getUpdateCount)
    if( updateStmt.getUpdateCount == 0 ){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temp)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2B8", "root", "123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (sensor, temperature) values (?, ?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temperature = ? where sensor = ?")
  }
}
