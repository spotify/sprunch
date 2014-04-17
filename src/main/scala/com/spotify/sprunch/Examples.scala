package com.spotify.sprunch

import org.apache.crunch.{PCollection, Pair=>CPair}
import Sprunch.Upgrades._
import Sprunch.Avro._
import scala.collection.JavaConversions._
import com.spotify.example.records.{CountryArtistPlays, TrackPlayedMessage}

object Examples {
  /** Outputs unique "words" in input along with the number of occurrences in the form "word:count" */
  def wordCount(lines: PCollection[String]) =
    lines.flatMap(_.split("\\s+"))
         .count()
         .map(wordCount => wordCount.first() + ":" + wordCount.second())

  /** Count the number of plays for each distinct pair of userCountry and artistName */
  def countryArtistPlays(messages: PCollection[TrackPlayedMessage]) =
    messages.map(msg => CPair.of(msg.getUserCountry, msg.getArtistName))
            .count()
            .map(rec => new CountryArtistPlays(rec.first().first(),
                                               rec.first().second(),
                                               rec.second()))

  /** Sum the total plays for each country using CountryArtistPlays as a starting point */
  def sumPlaysByCountry(records: PCollection[CountryArtistPlays]) =
    records.mapToTable(rec => (rec.getCountry, rec.getPlays))
           .groupByKey()
           .foldValues(0L, _+_)
           .map(countryPlays => countryPlays.first() + ":" + countryPlays.second())

}
