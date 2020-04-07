package com.github.snorochevskiy.playing.spark.grouping

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.scalatest.FunSuite

class FlatMapGroupsWithStateTest extends FunSuite {

  test("Testing flatMapGroupsWithState") {

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
        .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.NoTimeout())(aggFun)

    grouped.printSchema()

    grouped.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .start()
      .awaitTermination()

    // Will print
    // +--------+----+------------+
    // |location|temp|diffWithPrev|
    // +--------+----+------------+
    // |  Odessa|  22|           2|
    // |  Odessa|  20|           0|
    // |  Odessa|  20|           1|
    // |  Odessa|  19|           2|
    // |  Odessa|  17|          -1|
    // |  Odessa|  18|           1|
    // |  Odessa|  17|          -2|
    // |  Odessa|  19|           1|
    // |  Odessa|  18|           3|
    // |  Odessa|  15|           1|
    // |  Odessa|  14|          -1|
    // |  Odessa|  15|           5|
    // |  Odessa|  10|           0|
    // |  Odessa|  10|          -2|
    // |  Odessa|  12|           0|
    // |  Odessa|  12|          -1|
    // |  Odessa|  13|          -3|
    // |  Odessa|  16|           2|
    // |  Odessa|  14|           1|
    // |  Odessa|  13|          -3|
    // +--------+----+------------+
  }

  def aggFun: (String, Iterator[Temperature], GroupState[TemperatureDiff]) => Iterator[TemperatureDiff] =
    (key: String, it: Iterator[Temperature], state: GroupState[TemperatureDiff]) => {
      var stateOp = state.getOption

      var lst: List[TemperatureDiff] = List()
      for (input <- it) {
        stateOp = stateOp.map(s => s.copy(diffWithPrev = input.temp - s.temp, temp = input.temp))
          .orElse(Option(TemperatureDiff(key, input.temp, 0)))
        state.update(stateOp.get)
        lst = stateOp.get :: lst
      }

      lst.iterator
  }

}
