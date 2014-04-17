Sprunch
=======

A minimalist Scala API on top of Apache Crunch ( http://crunch.apache.org ) with the aim of removing boilerplate from
Java + Crunch whilst adding as little complexity as possible.

The two features which Sprunch aims to provide on top of Crunch are:

1. Expression of MapFn, DoFn, CombineFn and FilterFn in terms of lambda expressions
2. Implicit resolution of PTypes at compile time, so no need to specify a destination PType for your operation

```scala

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
            .map(countryArtistPlays => new CountryArtistPlays(countryArtistPlays.first().first(), countryArtistPlays.first().second(), countryArtistPlays.second()))

  /** Sum the total plays for each country using CountryArtistPlays as a starting point */
  def sumPlaysByCountry(records: PCollection[CountryArtistPlays]) =
    records.mapToTable(rec => (rec.getCountry, rec.getPlays))
           .groupByKey()
           .foldValues(0L, _+_)
           .map(countryPlays => countryPlays.first() + ":" + countryPlays.second())

}

```

Differences from Scrunch
-----

If you follow the big data world, you probably know that Crunch already has a Scala API called Scrunch.

* Sprunch resolves types at compile time rather than using runtime reflection. This means that mapping to an type which
  cannot be stored in HDFS will produce a compile error. This means we do not support reflected types, as in our
  environment we store all data in Avro SpecificRecords.
* All Sprunch methods return standard Crunch PCollections, PTables and PGroupedTables. Sprunch operations are
  implemented as "Pimp My Library"-style extensions to these classes. This means you can always fall back to using
  normal Crunch methods without the need for explicit conversions.
* Sprunch does not have it's own versions of Pipeline or any of its implementations or dependent classes. This means the
  total functional code size of Sprunch is around 70 lines.
