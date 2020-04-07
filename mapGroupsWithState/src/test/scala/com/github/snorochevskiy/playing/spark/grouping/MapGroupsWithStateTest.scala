package com.github.snorochevskiy.playing.spark.grouping

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.scalatest.FunSuite

class MapGroupsWithStateTest extends FunSuite with Serializable {

  test("Testing mapGroupsWithState") {

    val projectRoot = new java.io.File(".").getCanonicalPath
    val dataDir = s"$projectRoot/src/test/resources/flatMapGroupsWithState/"

    val sparkSession = SparkSession.builder()
      .appName("flatMapGroupsWithState_test")
      .master("local")
      .getOrCreate()

    import sparkSession.implicits._
    val df = sparkSession.readStream
      .format("text")
      .load( dataDir)

    val ds = df.as[String]

    val temperatureDs: Dataset[Temperature] = ds.map{ str =>
      val Array(location, temp) = str.split(" ")
      Temperature(location, temp.toInt)
    }

    temperatureDs.printSchema()

    val grouped = temperatureDs.groupByKey(_.location)
        .mapGroupsWithState(GroupStateTimeout.NoTimeout())(aggFun)

    grouped.printSchema()

    grouped.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .start()
      .awaitTermination()

    // The output will be
    // +--------+----+------------+
    // |location|temp|diffWithPrev|
    // +--------+----+------------+
    // |  Odessa|  22|           2|
    // +--------+----+------------+
  }

  val aggFun: (String, Iterator[Temperature], GroupState[TemperatureDiff]) => TemperatureDiff =
    (key: String, it: Iterator[Temperature], state: GroupState[TemperatureDiff]) => {
      var stateOp = state.getOption

      for (input <- it) {
        stateOp = stateOp.map(s => s.copy(diffWithPrev = input.temp - s.temp, temp = input.temp))
          .orElse(Option(TemperatureDiff(key, input.temp, 0)))
        state.update(stateOp.get)
      }

      stateOp.get
  }

}

