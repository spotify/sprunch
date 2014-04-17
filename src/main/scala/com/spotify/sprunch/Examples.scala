package com.spotify.sprunch

import org.apache.crunch.PCollection
import Sprunch.Upgrades._
import Sprunch.Avro._
import scala.collection.JavaConversions._

object Examples {
  /** Outputs unique "words" in input along with the number of occurrences */
  def wordCount(lines: PCollection[String]) =
    lines.flatMap(_.split("\\s+")).count().map(wordCount => wordCount.first() + ":" + wordCount.second())

}
