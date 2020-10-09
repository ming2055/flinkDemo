package apitest

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichReduceFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.util.Collector


object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    //    env.setStateBackend(new MemoryStateBackend())
    //    env.setStateBackend(new FsStateBackend(""))
//        env.setStateBackend(new RocksDBStateBackend(""))
        env.enableCheckpointing(1000L)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    //    env.getCheckpointConfig.setCheckpointTimeout(60000L)
          //设置checkpoint最大并发数
    //    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
          //两个checkpoint之间的时间间隔最小值
    //    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    //默认任务失败后会自动清掉checkpoint状态,RETAIN_ON_CANCELLATION即使手动取消也不删除
//    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
//        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    //    重启策略 固定延迟重启
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L))
    //    env.setRestartStrategy(RestartStrategies.failureRateRestart(5,Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))


    val inputStream: DataStream[String] = env.socketTextStream("172.31.40.135", 8888)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val resultStream: DataStream[(String, Double, Double)] = dataStream
      .keyBy(_.id)
      //      .reduce(new MyStateTestFunc())
      .flatMap(new TempChangeAlert(10.0))

    val resultStream2: DataStream[(String, Double, Double)] = dataStream
      .keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double]({
        //如果没有状态的话，也就是没有数据来过，那么就将当前数据温度值存入状态
        case (input: SensorReading, None) => (List.empty, Some(input.temp))
          //如果有状态，就应该与上次的温度值进行比较，如果大于阈值就输出报警
        case (input: SensorReading, lastTemp: Some[Double]) =>
          val diff = (input.temp - lastTemp.get).abs
          if (diff > 10.0) {
            (List((input.id, lastTemp.get, input.temp)), Some(input.temp))
          } else {
            (List.empty, Some(input.temp))
          }
      }
      )

    dataStream.print("data")
//    resultStream.print("result")
    resultStream2.print("result2")
    env.execute("state test")
  }
}

class MyStateTestFunc() extends RichReduceFunction[SensorReading] {
  lazy val myValueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("myvalue", classOf[Double]))
  lazy val myListState: ListState[String] = getRuntimeContext.getListState(new ListStateDescriptor[String]("mylist", classOf[String]))
  lazy val myMaoState: MapState[String, Int] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Int]("MAMAP", classOf[String], classOf[Int]))
  lazy val myReducingState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("myReduce", new MyReduceFunc, classOf[SensorReading]))

  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    val myValue: Double = myValueState.value()
    import scala.collection.JavaConversions._
    val myList: Iterable[String] = myListState.get()
    myMaoState.get("sensor_1")
    myReducingState.get()

    myValueState.update(0.0)
    myListState.add("hello flink")
    myMaoState.put("sensor_1", 1)
    myReducingState.add(t1)

    t1
  }
}

class TempChangeAlert(thredshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  // 定义一个状态，用来保存上一次的温度值
  //  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )
  private var lastTempState: ValueState[Double] = _
  private var isFirstTempState: ValueState[Boolean] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    isFirstTempState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-firsttemp", classOf[Boolean], true))
  }

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    // 获取状态，拿到上次的温度值
    val lastTemp = lastTempState.value()
//    println(getRuntimeContext.getTaskName)
//    println(getRuntimeContext.getIndexOfThisSubtask)
    lastTempState.update(in.temp)

    // 对比两次温度值，如果大于阈值，报警
    val diff = (in.temp - lastTemp).abs
    if (diff > thredshold && !isFirstTempState.value()) {
      collector.collect((in.id, lastTemp, in.temp))
    }
    isFirstTempState.update(false)

  }

}
