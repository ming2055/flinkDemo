package sinktest

import apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val inputStream: DataStream[String] = env.readTextFile("F:\\learning\\code\\flink_learning\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream
      .map( data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper()))

    env.execute("redis sink job")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading]{
  //定义保存数据到redis的命令 ，hash表
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET,"sensor_tmp")

  //定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = t.temp.toString
  //定义保存到redis的key
  override def getKeyFromData(t: SensorReading): String = t.id
}
