Disclaimer: Sprunch is currently experimental software, and we make no guarantees about ongoing support and development.

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
         .map{case (word, count) => word + ":" + count}

  /** Count the number of plays for each distinct pair of userCountry and artistName */
  def countryArtistPlays(messages: PCollection[TrackPlayedMessage]) =
    messages.map(msg => (msg.getUserCountry, msg.getArtistName))
            .count()
            .map{case ((country, artist), plays) =>
              new CountryArtistPlays(country, artist, plays)}



  /** Sum the total plays for each country using CountryArtistPlays as a starting point */
  def sumPlaysByCountry(records: PCollection[CountryArtistPlays]) =
    records.mapToTable(rec => (rec.getCountry, rec.getPlays))
           .groupByKey()
           .foldValues(0L, _+_)
           .map{case (country, plays) => country + ":" + plays}

  /** Output all TrackPlayedMessages for which userCountry="SE" and duration > 30 seconds */
  def filterTrackPlayedMessages(message: PCollection[TrackPlayedMessage]) =
    message.filterBy(msg => msg.getUserCountry.equals("SE") && msg.getDurationMs > 30000)

}

```

Operations and function naming
-----

Sprunch uses different names for the the standard Crunch operations it wraps, this is because the Scala compiler
will not attempt to upgrade an object via an implicit if the method being called has the same name as one defined on
the underlying class.

Sprunch Operation | Crunch Equivalent
------------------|------------------
map               | parallelDo with MapFn
flatMap           | parallelDo with DoFn
mapToTable        | parallelDo with MapFn and PTable output
extractKey        | by
filterBy          | filter
foldValues        | (subset of) combine with Aggregator

Usage
-----

* Import Sprunch.Upgrades._ and Sprunch.Avro._ (or write your own implicit PTypes for another family).
* Use new functions on PCollection, PTable and PGroupedTable types.

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


Differences from Scalding
-----

If you follow the big data world, you probably know that Hadoop already has a Scala API called Scalding (built on Cascading)

* Sprunch works with real types rather than tuples, giving you more compile-time safety and a programming environment
  more like conventional functional programming with Scala.
* Sprunch can run on other execution engines (such as Spark) because Crunch can.
* Sprunch has excellent support for local testing because Crunch does.