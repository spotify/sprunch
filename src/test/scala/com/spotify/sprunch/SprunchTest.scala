package com.spotify.sprunch
import Sprunch.Upgrades._
import Sprunch.Avro._
import org.junit.{Assert, Test}
import org.apache.crunch.impl.mem.MemPipeline
import org.apache.crunch.types.avro.Avros
import org.apache.crunch.PCollection
import scala.collection.JavaConverters._

class SprunchTest {
  @Test
  def testScalaPrimitives() {
    val p:PCollection[Integer] = MemPipeline.typedCollectionOf(Avros.ints(),1,2,3)
    val ints = p.map(i => i.intValue()).materialize().asScala.toList
    Assert.assertEquals(Seq(1,2,3), ints)
    val longs = p.map(i => i.longValue()).materialize().asScala.toList
    Assert.assertEquals(Seq(1L,2L,3L), longs)
    val floats = p.map(i => i.floatValue()).materialize().asScala.toList
    Assert.assertEquals(Seq(1f,2f,3f), floats)
    val doubles = p.map(i => i.doubleValue()).materialize().asScala.toList
    Assert.assertEquals(Seq(1d,2d,3d), doubles)
  }
}
