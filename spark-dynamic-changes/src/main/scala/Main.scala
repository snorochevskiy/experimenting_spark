package com.github.snorochevskiy.playing.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Main {

  def main(args: Array[String]): Unit = {
    var adder = 1

    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          adder += 1
          Thread.sleep(5000)
        }
      }
    }).start()

    val sparkSession = SparkSession.builder()
      .appName("dynamic-experiments")
      .getOrCreate()

    import sparkSession.implicits._

    val inputStream = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .selectExpr("cast (value as string) as json")
      .select(from_json($"json",schema).as("data"))
      .select("data.*").as[InputMsg]

    val transformedStream = inputStream.map(m => m.copy(counter = m.counter + adder))

    val outputQuery = transformedStream.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .queryName("MyProcessing")
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    sparkSession.streams.awaitAnyTermination()
  }

  val schema = StructType(
    Seq(
      StructField("counter", IntegerType, nullable = true)
    )
  )
}

case class InputMsg(counter: Int)