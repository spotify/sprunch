package com.spotify.sprunch

import org.junit.{Assert, Test}
import org.apache.crunch.impl.mem.MemPipeline
import org.apache.crunch.types.avro.Avros
import scala.collection.JavaConverters._

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
}
