package com.github.snorochevskiy.playing.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Main {

  def main(args: Array[String]): Unit = {
    var adder = 1

    val sparkSession = SparkSession.builder()
      .appName("dynamic-experiments")
      .getOrCreate()

    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = adder += 1
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    })

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