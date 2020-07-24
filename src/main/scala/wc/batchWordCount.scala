package wc

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * @Author Wu Ming
 * @Date 2020/6/15 14:07
 * @Version 1.0
 */
object batchWordCount {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val inputFilePath: String = "E:\\myproject\\flinkDemo\\src\\main\\resources\\data.txt"
    val inputDataSet: DataSet[String] = environment.readTextFile(inputFilePath)

    val resultDataSet: DataSet[(String,Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    resultDataSet.print()
  }

}
