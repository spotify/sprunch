package com.spotify.sprunch

import org.junit.{Assert, Test}
import org.apache.crunch.impl.mem.MemPipeline
import org.apache.crunch.types.avro.Avros
import scala.collection.JavaConverters._
import com.spotify.example.records.{CountryArtistPlays, TrackPlayedMessage}

class ExamplesTest {
  @Test
  def testWordCount() {
    val output = Examples.wordCount(
      MemPipeline.typedCollectionOf(
        Avros.strings(),
        "goodbye cruel world",
        "goodbye world",
        "cruel world")).materialize()
    val expected = Set("goodbye:2", "cruel:2", "world:3")
    Assert.assertEquals(expected, output.asScala.toSet)
  }

  @Test
  def testCountryArtistPlays() {
    val output = Examples.countryArtistPlays(
      MemPipeline.typedCollectionOf(Avros.specifics(classOf[TrackPlayedMessage]),
        new TrackPlayedMessage("trk", "Heretics", "UK", 60000),
        new TrackPlayedMessage("trk", "Heretics", "SE", 60000),
        new TrackPlayedMessage("trk", "Heretics", "SE", 60000),
        new TrackPlayedMessage("trk", "Demoscene Time Machine", "DE", 60000),
        new TrackPlayedMessage("trk", "Demoscene Time Machine", "DE", 60000)
    )).materialize()
    val expected = Set(
      new CountryArtistPlays("UK", "Heretics", 1L),
      new CountryArtistPlays("SE", "Heretics", 2L),
      new CountryArtistPlays("DE", "Demoscene Time Machine", 2L)
    )
    Assert.assertEquals(expected, output.asScala.toSet)
  }

  @Test
  def testSumPlaysByCountry() {
    val output = Examples.sumPlaysByCountry(
      MemPipeline.typedCollectionOf(Avros.specifics(classOf[CountryArtistPlays]),
        new CountryArtistPlays("SE", "Demoscene Time Machine", 100L),
        new CountryArtistPlays("SE", "bow church", 50L),
        new CountryArtistPlays("US", "bow chruch", 75L))
    ).materialize()
    val expected = Set(
      "SE:150", "US:75"
    )
    Assert.assertEquals(expected, output.asScala.toSet)
  }
}
