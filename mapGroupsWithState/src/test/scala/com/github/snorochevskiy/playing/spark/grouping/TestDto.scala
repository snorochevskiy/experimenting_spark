package com.github.snorochevskiy.playing.spark.grouping

case class Temperature(
  location: String,
  temp: Int
)

case class TemperatureDiff(
  location: String,
  temp: Int,
  diffWithPrev: Int
)